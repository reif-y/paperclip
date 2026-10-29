import { describe, expect, it } from "vitest";
import { taskPanelActivation } from "./plugin-task-panels.js";

describe("structured task panel activation", () => {
  it("counts Unicode code points and matches the schema's control-character rules", () => {
    for (const slotId of ["视".repeat(60), "😀".repeat(160), "view-0", "\u0085"]) {
      const panel = { pluginKey: "example.live-view", slotId };
      expect(taskPanelActivation(panel)).toEqual(panel);
    }
    for (const slotId of ["😀".repeat(161), "\u001f", "\u007f"]) {
      expect(taskPanelActivation({ pluginKey: "example.live-view", slotId })).toBeNull();
    }
  });
  it("accepts only bounded panel identifiers without caller run identity", () => {
    const panel = { pluginKey: "example.live-view", slotId: "screen" };
    expect(taskPanelActivation(panel)).toEqual(panel);
    for (const payload of [null, { output: "[paperclip-task-panel:example.live-view:screen]" },
      { ...panel, runId: "caller-run" }, { ...panel, slotId: "" }, { ...panel, slotId: "x".repeat(161) },
      { ...panel, pluginKey: "invalid\nkey" }]) expect(taskPanelActivation(payload)).toBeNull();
  });
});
