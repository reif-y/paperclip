import { useEffect, useMemo, useRef } from "react";
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import type { Issue } from "@paperclipai/shared";
import { api, ApiError } from "@/api/client";
import { heartbeatsApi } from "@/api/heartbeats";
import { useHasCompanyLiveConnection, useTranscriptLiveStream } from "@/context/LiveUpdatesProvider";
import { queryKeys } from "@/lib/queryKeys";
import { usePluginSlots, type ResolvedPluginSlot } from "@/plugins/slots";

export interface TaskPluginPanel {
  entityId: string;
  runId: string;
  slot: ResolvedPluginSlot;
}

const EMPTY_PANELS: TaskPluginPanel[] = [];

export function taskPluginPanelId(panel: TaskPluginPanel) {
  return `plugin:${panel.slot.pluginKey}:${panel.slot.id}:${panel.entityId}`;
}

/** Plugins own availability and content; the host supplies the running agent. */
export function useTaskPluginPanels(issue: Issue | null | undefined, enabled = true): TaskPluginPanel[] {
  const queryClient = useQueryClient();
  const savedTask = enabled && !!issue && !issue.id.startsWith("chat:");
  const connected = useHasCompanyLiveConnection(issue?.companyId);
  const { slots, errorMessage } = usePluginSlots({
    slotTypes: ["detailTab"], entityType: "agent", taskSidePanel: true,
    companyId: issue?.companyId, enabled: savedTask,
  });
  const { data: runs, isError: runsError } = useQuery({
    queryKey: queryKeys.issues.liveRuns(issue?.id ?? "pending"),
    queryFn: () => heartbeatsApi.liveRunsForIssue(issue!.id),
    enabled: savedTask && slots.length > 0 && !errorMessage,
    refetchInterval: connected ? false : 3000,
    retry: false,
  });
  const run = runs?.find(run => run.status === "running"
    && (!issue?.executionRunId || run.id === issue.executionRunId));
  const active = savedTask && slots.length > 0 && !!run && !runsError && !errorMessage;
  const activationKey = ["task-plugin-panel-activations", issue?.companyId, issue?.id, run?.id, run?.agentId];
  const { data: activations = [], isError: activationsError } = useQuery({
    queryKey: activationKey,
    queryFn: ({ signal }) => heartbeatsApi.taskPanelActivations(run!.id, { signal, cache: "no-store" }),
    enabled: active,
    retry: false,
  });
  const stream = useTranscriptLiveStream(active ? [run!.id] : [], { companyId: issue?.companyId, enabled: active }, event => {
    if (!active || !run || event.companyId !== issue?.companyId || event.payload.runId !== run.id
      || event.payload.agentId !== run.agentId || event.payload.issueId !== issue?.id
      || event.type !== "heartbeat.run.event" || event.payload.eventType !== "plugin.task_panel") return;
    const panel = event.payload.payload as { pluginKey?: unknown; slotId?: unknown } | null;
    if (typeof panel?.pluginKey !== "string" || typeof panel.slotId !== "string") return;
    void queryClient.cancelQueries({ queryKey: activationKey }).then(() => queryClient.setQueryData<Array<{ pluginKey: string; slotId: string }>>(activationKey, previous => {
      if (previous?.some(item => item.pluginKey === panel.pluginKey && item.slotId === panel.slotId)) return previous;
      return [...(previous ?? []), { pluginKey: panel.pluginKey as string, slotId: panel.slotId as string }];
    }));
  });
  const previousStream = useRef(stream);
  useEffect(() => {
    const previous = previousStream.current;
    previousStream.current = stream;
    if (active && stream.healthy && (!previous.healthy || previous.epoch !== stream.epoch)) {
      void queryClient.invalidateQueries({ queryKey: ["task-plugin-panel", issue?.companyId, issue?.id, run?.id, run?.agentId] });
      void queryClient.invalidateQueries({ queryKey: ["task-plugin-panel-activations", issue?.companyId, issue?.id, run?.id, run?.agentId] });
    }
  }, [active, stream.healthy, stream.epoch, queryClient, issue?.companyId, issue?.id, run?.id, run?.agentId]);
  const availability = useQueries({ queries: slots.map(slot => ({
    queryKey: ["task-plugin-panel", issue?.companyId, issue?.id, run?.id, run?.agentId, slot.pluginKey, slot.id, slot.pluginVersion],
    queryFn: async ({ signal }: { signal: AbortSignal }) => {
      const result = await api.get<{ available: boolean }>(
        `/plugins/${encodeURIComponent(slot.pluginId)}/api${slot.taskSidePanel!.availabilityPath.replace(":entityId", encodeURIComponent(run!.agentId))}?runId=${encodeURIComponent(run!.id)}&issueId=${encodeURIComponent(issue!.id)}`,
        { signal, cache: "no-store" },
      );
      if (result.available !== true) throw new Error("Plugin panel is not ready");
      return result;
    },
    enabled: active && !activationsError && activations.some(panel => panel.pluginKey === slot.pluginKey && panel.slotId === slot.id),
    retry: (failures: number, error: Error) => failures < 2 && !(error instanceof ApiError && error.status >= 400 && error.status < 500),
    retryDelay: 1000,
  })) });
  const available = availability.map((result, index) => !activationsError && !result.isError && result.data?.available === true
    && activations.some(panel => panel.pluginKey === slots[index]?.pluginKey && panel.slotId === slots[index]?.id));
  const availableKey = available.join(",");
  return useMemo(() => active && run ? slots.flatMap((slot, index) => available[index]
    ? [{ entityId: run.agentId, runId: run.id, slot }] : []) : EMPTY_PANELS,
  [active, run?.agentId, run?.id, slots, availableKey]);
}
