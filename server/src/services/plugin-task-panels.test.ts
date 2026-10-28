import { describe, expect, it } from "vitest";
import { taskPanelActivation } from "./plugin-task-panels.js";

describe("structured task panel activation", () => {
  it("accepts only bounded panel identifiers without caller run identity", () => {
    const panel = { pluginKey: "example.live-view", slotId: "screen" };
    expect(taskPanelActivation(panel)).toEqual(panel);
    for (const payload of [null, { output: "[paperclip-task-panel:example.live-view:screen]" },
      { ...panel, runId: "caller-run" }, { ...panel, slotId: "" }, { ...panel, slotId: "x".repeat(161) },
      { ...panel, pluginKey: "invalid\nkey" }]) expect(taskPanelActivation(payload)).toBeNull();
  });
});
