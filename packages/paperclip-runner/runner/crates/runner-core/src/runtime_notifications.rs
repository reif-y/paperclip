//! Local UI hints share Runner's durable, authenticated event transport.
use std::collections::HashSet;
use std::fs::{self, DirBuilder};
use std::io::{self, Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::durable::{
    create_private_temporary_file, open_private_regular_file, verify_private_directory,
    DurableRunnerConfig, DurableRunnerError, DurableState, DurableStateStore, EventPriority,
};

pub const CONTEXT_ENV: &str = "PAPERCLIP_RUNTIME_NOTIFICATION_CONTEXT";
pub const HELPER_ENV: &str = "PAPERCLIP_RUNTIME_NOTIFICATION_HELPER";
const MAX_FRAME: usize = 2048;

pub fn context_path(state_dir: &Path) -> PathBuf {
    state_dir.join("runtime-notifications.json")
}

struct MailboxDirectory {
    directory: PathBuf,
}

impl MailboxDirectory {
    fn create(parent: &Path) -> io::Result<Self> {
        let directory = parent.join(format!("prn-{}", Uuid::new_v4().simple()));
        DirBuilder::new().mode(0o700).create(&directory)?;
        Ok(Self { directory })
    }
}

impl Drop for MailboxDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn write_frame(path: &Path, frame: &Value) -> Result<(), DurableRunnerError> {
    let bytes = serde_json::to_vec(frame).map_err(failure)?;
    if bytes.len() > MAX_FRAME {
        return Err(failure("oversized notification"));
    }
    let (temporary, mut file) = create_private_temporary_file(path)?;
    let result = file
        .write_all(&bytes)
        .and_then(|_| fs::rename(&temporary, path));
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result.map_err(failure)
}

fn read_frame(path: &Path) -> Result<Value, DurableRunnerError> {
    if !fs::symlink_metadata(path).map_err(failure)?.is_file() {
        return Err(failure("notification frame must be a regular file"));
    }
    let mut bytes = Vec::new();
    open_private_regular_file(path)
        .map_err(failure)?
        .take(MAX_FRAME as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(failure)?;
    if bytes.len() > MAX_FRAME {
        return Err(failure("oversized notification"));
    }
    serde_json::from_slice(&bytes).map_err(failure)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Activation {
    context: String,
    plugin_key: String,
    slot_id: String,
}

pub struct RuntimeNotifications {
    local: MailboxDirectory,
    run_id: String,
    context: String,
    activated: HashSet<(String, String)>,
}

impl RuntimeNotifications {
    pub fn bind(state_dir: &Path) -> Result<Self, DurableRunnerError> {
        verify_private_directory(state_dir)?;
        let local = MailboxDirectory::create(&std::env::temp_dir()).map_err(failure)?;
        let path = context_path(state_dir);
        write_frame(&path, &json!({"mailbox": local.directory}))?;
        Ok(Self {
            local,
            run_id: String::new(),
            context: String::new(),
            activated: HashSet::new(),
        })
    }

    // Process on the authenticated control loop, never under caller-supplied run identity.
    pub fn poll(
        &mut self,
        config: &DurableRunnerConfig,
        state: &mut DurableState,
        store: &DurableStateStore,
    ) {
        if self.run_id != config.run_id {
            self.run_id.clone_from(&config.run_id);
            self.context = Uuid::new_v4().to_string();
            self.activated.clear();
        }
        let Ok(entries) = fs::read_dir(&self.local.directory) else {
            return;
        };
        for entry in entries.take(16).flatten() {
            let directory = entry.path();
            if verify_private_directory(&directory).is_err() {
                continue;
            }
            // A killed publisher must not leave a mailbox that starves later requests.
            if fs::metadata(&directory)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|time| time.elapsed().ok())
                .is_some_and(|age| age > Duration::from_secs(2))
            {
                let _ = fs::remove_dir_all(&directory);
                continue;
            }
            let path = directory.join("request.json");
            let request = read_frame(&path)
                .ok()
                .and_then(|value| serde_json::from_value::<Request>(value).ok());
            let _ = fs::remove_file(path);
            let Some(request) = request.filter(|request| request.jsonrpc == "2.0") else {
                continue;
            };
            let result = if state.lifecycle != "ready" || state.warm_transition.is_some() {
                Err(DurableRunnerError::invalid(
                    "runtime authority is not active",
                ))
            } else {
                self.handle(
                    request.method.as_str(),
                    request.params,
                    config,
                    state,
                    store,
                )
            };
            let reply = match result {
                Ok(result) => json!({"jsonrpc":"2.0", "id":request.id, "result":result}),
                Err(_) => {
                    json!({"jsonrpc":"2.0", "id":request.id, "error":{"code":-32602,"message":"notification rejected"}})
                }
            };
            let _ = write_frame(&directory.join("reply.json"), &reply);
        }
    }

    fn handle(
        &mut self,
        method: &str,
        params: Value,
        config: &DurableRunnerConfig,
        state: &mut DurableState,
        store: &DurableStateStore,
    ) -> Result<Value, DurableRunnerError> {
        if method == "taskPanel/context" && params == json!({}) {
            return Ok(json!({"context":self.context}));
        }
        if method != "taskPanel/activate" {
            return Err(failure("unknown notification"));
        }
        let panel: Activation = serde_json::from_value(params).map_err(failure)?;
        let valid = |value: &str| {
            !value.is_empty()
                && value.chars().count() <= 160
                && !value.chars().any(|ch| ch.is_ascii_control())
        };
        if panel.context != self.context || !valid(&panel.plugin_key) || !valid(&panel.slot_id) {
            return Err(failure("invalid notification context or panel"));
        }
        let key = (panel.plugin_key, panel.slot_id);
        if !self.activated.contains(&key) {
            if self.activated.len() >= 128 {
                return Err(failure("notification limit reached"));
            }
            // Commit before replying; a failed hint must not poison the live outbox.
            let mut next = state.clone();
            next.enqueue_event(
                config,
                "task.panel.activated",
                EventPriority::P1,
                json!({"pluginKey":key.0,"slotId":key.1}),
            )?;
            store.save(&next)?;
            *state = next;
            self.activated.insert(key);
        }
        Ok(json!({"accepted":true}))
    }
}

fn failure(error: impl std::fmt::Display) -> DurableRunnerError {
    DurableRunnerError::invalid(format!("runtime notification: {error}"))
}

fn mailbox_path(path: &Path) -> Result<PathBuf, DurableRunnerError> {
    let descriptor = read_frame(path)?;
    let directory = descriptor["mailbox"]
        .as_str()
        .ok_or_else(|| failure("missing notification mailbox"))?;
    let directory = PathBuf::from(directory);
    if !directory.is_absolute() {
        return Err(failure("notification mailbox must be absolute"));
    }
    verify_private_directory(&directory)?;
    Ok(directory)
}

/// Exact resources needed by a publisher; never grant the enclosing Runner state directory.
pub(crate) fn access_paths() -> Option<[PathBuf; 3]> {
    let descriptor = PathBuf::from(std::env::var_os(CONTEXT_ENV)?);
    let helper = PathBuf::from(std::env::var_os(HELPER_ENV)?);
    let directory = mailbox_path(&descriptor).ok()?;
    if !descriptor.is_absolute() || !helper.is_absolute() {
        return None;
    }
    Some([helper, descriptor, directory])
}

fn request(path: &Path, method: &str, params: Value) -> Result<Value, DurableRunnerError> {
    let directory = mailbox_path(path)?;
    // Filesystem RPC works in tool sandboxes without network access or Runner state access.
    let client = MailboxDirectory::create(&directory).map_err(failure)?;
    write_frame(
        &client.directory.join("request.json"),
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
    )?;
    let path = client.directory.join("reply.json");
    let deadline = Instant::now() + Duration::from_millis(500);
    while !path.try_exists().map_err(failure)? {
        if Instant::now() >= deadline {
            return Err(failure("notification timed out"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let reply = read_frame(&path)?;
    if reply["jsonrpc"] != "2.0" || reply["id"] != 1 || reply.get("error").is_some() {
        return Err(failure("notification rejected"));
    }
    reply
        .get("result")
        .cloned()
        .ok_or_else(|| failure("missing notification result"))
}

/// CLI helpers intentionally accept no run ID; the opaque context fences warm attachments.
pub fn run_cli(args: &[String]) -> Result<(), DurableRunnerError> {
    let path = std::env::var_os(CONTEXT_ENV)
        .ok_or_else(|| failure("notification context is unavailable"))?;
    match args {
        [command] if command == "notification-context" => {
            let result = request(Path::new(&path), "taskPanel/context", json!({}))?;
            let context = result["context"]
                .as_str()
                .ok_or_else(|| failure("missing activation context"))?;
            println!("{context}");
            Ok(())
        }
        [command, context, plugin, slot] if command == "notify-task-panel" => {
            request(
                Path::new(&path),
                "taskPanel/activate",
                json!({"context":context,"pluginKey":plugin,"slotId":slot}),
            )?;
            Ok(())
        }
        _ => Err(failure("invalid notification arguments")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn config(path: PathBuf) -> DurableRunnerConfig {
        DurableRunnerConfig {
            connect_url: "ws://127.0.0.1/runner".into(),
            ca_bundle_path: None,
            state_dir: path,
            runner_instance_id: "runner-1".into(),
            environment_lease_id: "lease-1".into(),
            run_id: "run-1".into(),
            normalized_session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            item_id: "item-1".into(),
            runner_version: "1".into(),
            runner_digest: "sha256:digest".into(),
            acpx_launch_profile: None,
            opencode_launch_profile: None,
            max_outbox_bytes: 1024 * 1024,
            p0_reserve_bytes: 64 * 1024,
            max_frame_bytes: 64 * 1024,
            reconnect_delay: Duration::from_millis(1),
            reconnect_grace: None,
            max_runtime: Duration::ZERO,
        }
    }

    fn rpc(
        server: &mut RuntimeNotifications,
        config: &DurableRunnerConfig,
        state: &mut DurableState,
        store: &DurableStateStore,
        method: &str,
        params: Value,
    ) -> Result<Value, DurableRunnerError> {
        let path = context_path(&config.state_dir);
        let method = method.to_owned();
        let client = thread::spawn(move || request(&path, &method, params));
        let start = Instant::now();
        while !client.is_finished() && start.elapsed() < Duration::from_secs(2) {
            server.poll(config, state, store);
            thread::sleep(Duration::from_millis(1));
        }
        client.join().unwrap()
    }

    #[test]
    #[ignore = "requires CODEX_NOTIFICATION_SANDBOX_COMMAND and PAPERCLIP_RUNTIME_NOTIFICATION_HELPER"]
    fn publisher_crosses_real_codex_tool_boundary_without_runner_state_access() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::sync::mpsc;
        let command = std::env::var_os("CODEX_NOTIFICATION_SANDBOX_COMMAND").unwrap();
        let helper = std::env::var_os(HELPER_ENV).unwrap();
        let directory = MailboxDirectory::create(&std::env::temp_dir()).unwrap();
        let config = config(directory.directory.clone());
        let store = DurableStateStore::new(&config.state_dir).unwrap();
        let (mut state, _) = store.load_or_create(&config).unwrap();
        state.lifecycle = "ready".into();
        let mut server = RuntimeNotifications::bind(&config.state_dir).unwrap();
        let paths = [
            PathBuf::from(&helper),
            context_path(&config.state_dir),
            server.local.directory.clone(),
        ];
        let workspace = config.state_dir.join("workspace");
        fs::create_dir(&workspace).unwrap();
        let home = workspace.join("home");
        fs::create_dir(&home).unwrap();
        let profile = "paperclip-runner-workspace-only";
        let mut args = vec![
            "-c".into(),
            format!("default_permissions=\"{profile}\""),
            "-c".into(),
            "shell_environment_policy.include_only=[\"PATH\",\"LANG\"]".into(),
        ];
        for name in [profile, "paperclip-runner-workspace-read-only"] {
            args.extend(["-c".into(), format!("permissions.{name}.filesystem={{\":root\"=\"none\",\":minimal\"=\"read\",\":tmpdir\"=\"none\",\":workspace_roots\"={{\".\"=\"read\"}}}}"),
                "-c".into(), format!("permissions.{name}.network.enabled=false")]);
        }
        let extra = crate::codex_provider::runtime_notification_codex_args(&args, profile, &paths);
        let script = r#"
[ -z "${UNRELATED_SECRET:-}" ] || exit 70
if cat "$1" >/dev/null 2>&1; then exit 71; fi
if /bin/sh -c ': >> "$PAPERCLIP_RUNTIME_NOTIFICATION_CONTEXT"' 2>/dev/null; then exit 72; fi
touch "$2/probe" || exit 75
rm "$2/probe" || exit 76
context=$("$PAPERCLIP_RUNTIME_NOTIFICATION_HELPER" notification-context) || exit 73
"$PAPERCLIP_RUNTIME_NOTIFICATION_HELPER" notify-task-panel "$context" example.live-view screen || exit 74
"#;
        let mut child = Command::new(command)
            .args(args)
            .args(extra)
            .args(["app-server", "--stdio", "--strict-config"])
            .current_dir(&workspace)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .env("HOME", &home)
            .env("CODEX_HOME", &home)
            .env("UNRELATED_SECRET", "must-not-reach-command")
            .env(CONTEXT_ENV, &paths[1])
            .env(HELPER_ENV, helper)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(Result::ok) {
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = tx.send(value);
                }
            }
        });
        writeln!(input, "{}", json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"notification-probe","version":"1"},"capabilities":{"experimentalApi":true}}})).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut response = None;
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline && response.is_none()
        {
            server.poll(&config, &mut state, &store);
            while let Ok(message) = rx.try_recv() {
                if message["id"] == 1 {
                    assert!(message.get("error").is_none(), "initialize: {message}");
                    writeln!(input, "{}", json!({"method":"initialized","params":{}})).unwrap();
                    writeln!(input, "{}", json!({"id":2,"method":"command/exec","params":{
                        "command":["/bin/sh","-c",script,"publisher",config.state_dir.join("runner-state.json"),paths[2]],
                        "cwd":workspace,"timeoutMs":10000}})).unwrap();
                } else if message["id"] == 2 {
                    response = Some(message);
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        if child.try_wait().unwrap().is_none() {
            child.kill().unwrap();
        }
        let result = child.wait_with_output().unwrap();
        reader.join().unwrap();
        let response = response.unwrap_or_else(|| {
            panic!(
                "app-server did not reply: {}",
                String::from_utf8_lossy(&result.stderr)
            )
        });
        assert!(response.get("error").is_none(), "command: {response}");
        assert_eq!(response["result"]["exitCode"], 0, "publisher: {response}");
        assert_eq!(state.outbox.len(), 1);
        assert_eq!(state.outbox[0].envelope["payload"]["runId"], config.run_id);
    }

    #[test]
    fn mailbox_frames_are_bounded_private_and_cleaned_after_timeout() {
        let root = MailboxDirectory::create(&std::env::temp_dir()).unwrap();
        let config = config(root.directory.clone());
        let store = DurableStateStore::new(&config.state_dir).unwrap();
        let (mut state, _) = store.load_or_create(&config).unwrap();
        let mut server = RuntimeNotifications::bind(&config.state_dir).unwrap();
        let client = MailboxDirectory::create(&server.local.directory).unwrap();
        let path = client.directory.join("request.json");
        assert!(write_frame(&path, &json!("x".repeat(MAX_FRAME))).is_err());
        fs::write(&path, "x".repeat(MAX_FRAME + 1)).unwrap();
        assert!(read_frame(&path).is_err());
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(config.state_dir.join("runner-state.json"), &path).unwrap();
        server.poll(&config, &mut state, &store);
        assert!(state.outbox.is_empty());
        assert!(config.state_dir.join("runner-state.json").is_file());
        drop(client);
        assert!(request(
            &context_path(&config.state_dir),
            "taskPanel/context",
            json!({})
        )
        .is_err());
        assert_eq!(fs::read_dir(&server.local.directory).unwrap().count(), 0);
        // Model a client killed after publishing but before receiving its reply.
        let orphan = MailboxDirectory::create(&server.local.directory).unwrap();
        let handle = fs::File::open(&orphan.directory).unwrap();
        let old = std::time::SystemTime::now() - Duration::from_secs(3);
        handle
            .set_times(fs::FileTimes::new().set_modified(old))
            .unwrap();
        server.poll(&config, &mut state, &store);
        assert!(!orphan.directory.exists());
    }

    #[test]
    fn activation_identifiers_match_schema_unicode_bounds() {
        let directory = MailboxDirectory::create(&std::env::temp_dir()).unwrap();
        let config = config(directory.directory.clone());
        let store = DurableStateStore::new(&config.state_dir).unwrap();
        let (mut state, _) = store.load_or_create(&config).unwrap();
        state.lifecycle = "ready".into();
        let mut server = RuntimeNotifications::bind(&config.state_dir).unwrap();
        let context = rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/context",
            json!({}),
        )
        .unwrap()["context"]
            .clone();
        for slot in [
            "视".repeat(60),
            "😀".repeat(160),
            "view-0".into(),
            "\u{0085}".into(),
        ] {
            rpc(
                &mut server,
                &config,
                &mut state,
                &store,
                "taskPanel/activate",
                json!({"context":context,"pluginKey":"example.live-view","slotId":slot}),
            )
            .unwrap();
        }
        for slot in [
            "😀".repeat(161),
            "".into(),
            "\u{001f}".into(),
            "\u{007f}".into(),
        ] {
            assert!(rpc(
                &mut server,
                &config,
                &mut state,
                &store,
                "taskPanel/activate",
                json!({"context":context,"pluginKey":"example.live-view","slotId":slot})
            )
            .is_err());
        }
    }

    #[test]
    fn structured_activation_is_durable_bounded_and_fenced_by_runner_authority() {
        let directory = MailboxDirectory::create(&std::env::temp_dir()).unwrap();
        let mut config = config(directory.directory.clone());
        let store = DurableStateStore::new(&config.state_dir).unwrap();
        let (mut state, _) = store.load_or_create(&config).unwrap();
        state.lifecycle = "ready".into();
        let mut server = RuntimeNotifications::bind(&config.state_dir).unwrap();
        let context = rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/context",
            json!({}),
        )
        .unwrap()["context"]
            .clone();
        let activation =
            json!({"context":context,"pluginKey":"example.live-view","slotId":"screen"});
        let state_path = config.state_dir.join("runner-state.json");
        let backup = config.state_dir.join("state-backup");
        fs::rename(&state_path, &backup).unwrap();
        fs::create_dir(&state_path).unwrap();
        assert!(rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            activation.clone()
        )
        .is_err());
        assert!(state.outbox.is_empty());
        fs::remove_dir(&state_path).unwrap();
        fs::rename(&backup, &state_path).unwrap();
        rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            activation.clone(),
        )
        .unwrap();
        rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            activation.clone(),
        )
        .unwrap();
        assert_eq!(state.outbox.len(), 1);
        let event = &state.outbox[0].envelope["payload"];
        assert_eq!(event["eventType"], "task.panel.activated");
        assert_eq!(event["runId"], "run-1");
        assert_eq!(event["sourceKind"], "runner");
        assert_eq!(
            event["payload"],
            json!({"pluginKey":"example.live-view","slotId":"screen"})
        );
        assert_eq!(
            store.load_or_create(&config).unwrap().0.outbox,
            state.outbox
        );
        let mut forged = activation.clone();
        forged["runId"] = json!("run-2");
        assert!(rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            forged
        )
        .is_err());
        let mut oversized = activation.clone();
        oversized["slotId"] = json!("x".repeat(161));
        assert!(rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            oversized
        )
        .is_err());
        config.run_id = "run-2".into();
        state.run_id.clone_from(&config.run_id);
        assert!(rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            activation
        )
        .is_err());
        let current = rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/context",
            json!({}),
        )
        .unwrap()["context"]
            .clone();
        assert_ne!(current, context);
        rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            json!({"context":current,"pluginKey":"example.live-view","slotId":"screen"}),
        )
        .unwrap();
        assert_eq!(state.outbox.len(), 2);
        assert_eq!(state.outbox[1].envelope["payload"]["runId"], "run-2");
        state.lifecycle = "suspended".into();
        assert!(rpc(
            &mut server,
            &config,
            &mut state,
            &store,
            "taskPanel/context",
            json!({})
        )
        .is_err());
        drop(server);
        // The stable descriptor survives process recovery; old contexts do not.
        let mut replacement = RuntimeNotifications::bind(&config.state_dir).unwrap();
        state.lifecycle = "ready".into();
        assert!(rpc(
            &mut replacement,
            &config,
            &mut state,
            &store,
            "taskPanel/activate",
            json!({"context":current,"pluginKey":"example.live-view","slotId":"screen"})
        )
        .is_err());
        let recovered = rpc(
            &mut replacement,
            &config,
            &mut state,
            &store,
            "taskPanel/context",
            json!({}),
        )
        .unwrap();
        assert_ne!(recovered["context"], current);
        drop(replacement);
        fs::remove_dir_all(&config.state_dir).unwrap();
    }
}
