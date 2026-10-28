import { and, eq, sql } from "drizzle-orm";
import { heartbeatRunEvents, type Db } from "@paperclipai/db";

import { appendHeartbeatRunEvent } from "./heartbeat-run-events.js";
import { publishLiveEvent } from "./live-events.js";
import { heartbeatRuns } from "@paperclipai/db";

export const TASK_PANEL_ACTIVATION_EVENT = "plugin.task_panel";
export interface TaskPanelActivation { pluginKey: string; slotId: string }

/** Output is a UI hint; availability and authorization stay with the plugin. */
export function observeTaskPanelActivations(activate: (panel: TaskPanelActivation) => Promise<void>) {
  const seen = new Set<string>();
  const tails = new Map<string, string>();
  const observe = async (channel: string, chunk: unknown, depth = 0): Promise<void> => {
    if (depth > 12) return;
    if (chunk && typeof chunk === "object") {
      for (const [key, value] of Object.entries(chunk)) await observe(`${channel}.${key}`, value, depth + 1);
      return;
    }
    if (typeof chunk !== "string") return;
    const previous = tails.get(channel) ?? "";
    const text = previous + chunk;
    let consumed = 0;
    if (!tails.has(channel) && tails.size >= 128) tails.delete(tails.keys().next().value!);
    tails.set(channel, text.slice(-1024));
    for (const match of text.matchAll(/(?:^|\n)\[paperclip-task-panel:([a-zA-Z0-9_.%~!-]{1,384}):([a-zA-Z0-9_.%~!-]{1,384})\](?=\r?\n|$)/g)) {
      consumed = match.index! + match[0].length;
      let panel: TaskPanelActivation;
      try { panel = { pluginKey: decodeURIComponent(match[1]!), slotId: decodeURIComponent(match[2]!) }; }
      catch { continue; }
      const key = JSON.stringify(panel);
      if (seen.has(key) || seen.size >= 128) continue;
      seen.add(key);
      try { await activate(panel); }
      catch (error) { seen.delete(key); tails.set(channel, previous); throw error; }
    }
    if (consumed) tails.set(channel, text.slice(consumed).slice(-1024));
  };
  return observe;
}

export async function readTaskPanelActivations(db: Db, companyId: string, runId: string): Promise<TaskPanelActivation[]> {
  const rows = await db.selectDistinct({ payload: heartbeatRunEvents.payload }).from(heartbeatRunEvents)
    .where(and(eq(heartbeatRunEvents.companyId, companyId), eq(heartbeatRunEvents.runId, runId),
      eq(heartbeatRunEvents.eventType, TASK_PANEL_ACTIVATION_EVENT))).limit(128);
  return rows.flatMap(({ payload }) => typeof payload?.pluginKey === "string" && typeof payload.slotId === "string"
    ? [{ pluginKey: payload.pluginKey, slotId: payload.slotId }] : []);
}

/** Source replay may repeat a hint; commit and publish each run/panel once. */
export async function recordTaskPanelActivation(db: Db, binding: {
  companyId: string; runId: string; agentId: string; issueId: string | null;
}, panel: TaskPanelActivation) {
  const row = await db.transaction(async tx => {
    const [run] = await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(and(
      eq(heartbeatRuns.id, binding.runId), eq(heartbeatRuns.companyId, binding.companyId), eq(heartbeatRuns.agentId, binding.agentId),
    )).for("update").limit(1);
    if (!run) throw new Error("task_panel_run_binding_mismatch");
    const [existing] = await tx.select({ id: heartbeatRunEvents.id }).from(heartbeatRunEvents).where(and(
      eq(heartbeatRunEvents.runId, run.id), eq(heartbeatRunEvents.eventType, TASK_PANEL_ACTIVATION_EVENT),
      sql`${heartbeatRunEvents.payload} @> ${JSON.stringify(panel)}::jsonb`,
    )).limit(1);
    if (existing) return null;
    const receipt = await appendHeartbeatRunEvent(tx as unknown as Db, { ...binding, eventType: TASK_PANEL_ACTIVATION_EVENT,
      stream: "system", payload: { ...panel } });
    return receipt.row;
  });
  if (row) publishLiveEvent({ companyId: binding.companyId, type: "heartbeat.run.event", payload: {
    ...binding, eventType: TASK_PANEL_ACTIVATION_EVENT, seq: row.seq, stream: "system", payload: { ...panel },
  } });
}
