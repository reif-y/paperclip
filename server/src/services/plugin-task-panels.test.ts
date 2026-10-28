import { describe, expect, it, vi } from "vitest";
import { observeTaskPanelActivations } from "./plugin-task-panels.js";
const marker = "[paperclip-task-panel:example.live-view:screen]";
describe("task panel activation hints", () => {
  it("assembles split output in one channel and records each panel once", async () => {
    const save = vi.fn(async () => {}), observe = observeTaskPanelActivations(save);
    await observe("stderr", marker.slice(0, 25));
    await observe("stdout", marker.slice(25)); expect(save).not.toHaveBeenCalled();
    await observe("stderr", marker.slice(25)); await observe("stderr", marker);
    expect(save).toHaveBeenCalledExactlyOnceWith({ pluginKey: "example.live-view", slotId: "screen" });
  });
  it("reads structured tool output and decodes contribution keys", async () => {
    const save = vi.fn(async () => {}), observe = observeTaskPanelActivations(save);
    await observe("event", { kind: "dynamicToolCall", item: { result: { content: [{ text: "[paperclip-task-panel:example.live-view:screen%3Aone]" }] } } });
    expect(save).toHaveBeenCalledWith({ pluginKey: "example.live-view", slotId: "screen:one" });
    await observe("event", "[paperclip-task-panel:example.live-view:%FF]");
    expect(save).toHaveBeenCalledTimes(1);
  });
  it("retries failed persistence instead of acknowledging an unsaved activation", async () => {
    const save = vi.fn().mockRejectedValueOnce(new Error("unavailable")).mockResolvedValue(undefined);
    const observe = observeTaskPanelActivations(save);
    await expect(observe("stderr", marker)).resolves.toBeUndefined();
    await observe("stderr", marker); expect(save).toHaveBeenCalledTimes(2);
  });
  it("keeps complete tool results separate even when the previous output has no newline", async () => {
    const save = vi.fn(async () => {}), observe = observeTaskPanelActivations(save);
    await observe("event", { output: "ready" });
    await observe("event", { output: marker });
    expect(save).toHaveBeenCalledExactlyOnceWith({ pluginKey: "example.live-view", slotId: "screen" });
  });
  it("bounds persistence retries without rejecting source output", async () => {
    const save = vi.fn().mockRejectedValue(new Error("unavailable")), observe = observeTaskPanelActivations(save);
    await expect(observe("stderr", marker)).resolves.toBeUndefined(); expect(save).toHaveBeenCalledTimes(3);
    await observe("stderr", `\n${marker}`); expect(save).toHaveBeenCalledTimes(6);
  });
  it("does not infer activation from ordinary commands or prose", async () => {
    const save = vi.fn(async () => {}), observe = observeTaskPanelActivations(save);
    await observe("event", { command: "show a plugin panel", output: "view ready" });
    await observe("event", `printf '${marker}\\n'`);
    expect(save).not.toHaveBeenCalled();
  });
});
