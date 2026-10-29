import { describe, expect, it } from "vitest";
import { validatePrpEvent, type PrpEvent } from "./replay-contract.js";

function event(version: 1 | 2 | 3, slotId: string): PrpEvent {
  return {
    schema: `paperclip.prp.event.v${version}`, schemaVersion: version,
    sourceEventId: "panel-1", sourceSeq: 1, sourceInstanceId: "runner", sourceKind: "runner",
    runId: "run", normalizedSessionId: "session", eventType: "task.panel.activated", priority: 1,
    emittedAt: "2099-01-01T00:00:00.000Z", payload: { pluginKey: "example.live-view", slotId },
  };
}

describe.each([1, 2, 3] as const)("PRP v%s task panel identifiers", version => {
  it("counts Unicode code points and excludes only ASCII controls", () => {
    for (const value of ["视".repeat(60), "😀".repeat(160), "view-0", "\u0085"]) {
      expect(validatePrpEvent(event(version, value)).ok).toBe(true);
    }
    for (const value of ["😀".repeat(161), "", "\u001f", "\u007f"]) {
      expect(validatePrpEvent(event(version, value)).ok).toBe(false);
    }
  });
});
