import { and, eq, ne, sql } from "drizzle-orm";
import { heartbeatRunEvents, plugins, type Db } from "@paperclipai/db";

export const TASK_PANEL_ACTIVATION_EVENT = "task.panel.activated";
export interface TaskPanelActivation { pluginKey: string; slotId: string }

export function taskPanelActivation(payload: Record<string, unknown> | null): TaskPanelActivation | null {
  const valid = (value: unknown): value is string => typeof value === "string" && value.length > 0
    && value.length <= 160 && !/[\u0000-\u001f\u007f]/.test(value);
  return payload && Object.keys(payload).length === 2 && valid(payload.pluginKey) && valid(payload.slotId)
    ? { pluginKey: payload.pluginKey, slotId: payload.slotId } : null;
}

/** The ordinary authenticated Runner event is the durable activation record. */
export async function readTaskPanelActivations(db: Db, companyId: string, runId: string): Promise<TaskPanelActivation[]> {
  const rows = await db.selectDistinct({
    payload: sql<Record<string, unknown>>`${heartbeatRunEvents.payload}->'prpEvent'->'payload'`, manifest: plugins.manifestJson,
  }).from(heartbeatRunEvents)
    .innerJoin(plugins, eq(plugins.pluginKey, sql`${heartbeatRunEvents.payload}->'prpEvent'->'payload'->>'pluginKey'`))
    .where(and(eq(heartbeatRunEvents.companyId, companyId), eq(heartbeatRunEvents.runId, runId),
      eq(heartbeatRunEvents.eventType, TASK_PANEL_ACTIVATION_EVENT), ne(plugins.status, "uninstalled"),
      sql`${heartbeatRunEvents.payload}->'prpEvent'->>'sourceKind' = 'runner'`,
      sql`${heartbeatRunEvents.payload}->'prpEvent'->>'eventType' = ${TASK_PANEL_ACTIVATION_EVENT}`)).limit(128);
  return rows.flatMap(({ payload, manifest }) => {
    const panel = taskPanelActivation(payload);
    return panel && manifest.ui?.slots?.some(slot => slot.id === panel.slotId && slot.type === "detailTab"
      && slot.entityTypes?.includes("agent") && slot.taskSidePanel) ? [panel] : [];
  });
}
