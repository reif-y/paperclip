//! Local UI hints share Runner's durable, authenticated event transport.
use std::collections::HashSet;
use std::fs::{self, DirBuilder};
use std::io::{self, Read, Write};
use std::os::unix::{fs::DirBuilderExt, net::UnixDatagram};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

struct LocalSocket {
    socket: UnixDatagram,
    directory: PathBuf,
}

impl LocalSocket {
    fn bind() -> io::Result<Self> {
        let directory = std::env::temp_dir().join(format!(
            "prn-{}",
            &Uuid::new_v4().simple().to_string()[..16]
        ));
        DirBuilder::new().mode(0o700).create(&directory)?;
        match UnixDatagram::bind(directory.join("s")) {
            Ok(socket) => Ok(Self { socket, directory }),
            Err(error) => {
                let _ = fs::remove_dir(&directory);
                Err(error)
            }
        }
    }
}

impl Drop for LocalSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.directory.join("s"));
        let _ = fs::remove_dir(&self.directory);
    }
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
    local: LocalSocket,
    run_id: String,
    context: String,
    activated: HashSet<(String, String)>,
}

impl RuntimeNotifications {
    pub fn bind(state_dir: &Path) -> Result<Self, DurableRunnerError> {
        verify_private_directory(state_dir)?;
        let local = LocalSocket::bind().map_err(failure)?;
        local.socket.set_nonblocking(true).map_err(failure)?;
        let path = context_path(state_dir);
        let (temporary, mut file) = create_private_temporary_file(&path)?;
        let written = (|| -> io::Result<()> {
            serde_json::to_writer(&mut file, &json!({"socket": local.directory.join("s")}))?;
            file.flush()?;
            fs::rename(&temporary, &path)
        })();
        if written.is_err() {
            let _ = fs::remove_file(temporary);
        }
        written.map_err(failure)?;
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
        let mut frame = [0; MAX_FRAME + 1];
        for _ in 0..16 {
            let (length, peer) = match self.local.socket.recv_from(&mut frame) {
                Ok(received) => received,
                Err(_) => break,
            };
            let Some(peer) = peer.as_pathname() else {
                continue;
            };
            let request = (length <= MAX_FRAME)
                .then(|| serde_json::from_slice::<Request>(&frame[..length]).ok())
                .flatten();
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
            let _ = self
                .local
                .socket
                .send_to(reply.to_string().as_bytes(), peer);
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
            !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
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

fn request(path: &Path, method: &str, params: Value) -> Result<Value, DurableRunnerError> {
    let mut descriptor = String::new();
    open_private_regular_file(path)
        .map_err(failure)?
        .take(MAX_FRAME as u64 + 1)
        .read_to_string(&mut descriptor)
        .map_err(failure)?;
    if descriptor.len() > MAX_FRAME {
        return Err(failure("oversized notification descriptor"));
    }
    let descriptor: Value = serde_json::from_str(&descriptor).map_err(failure)?;
    let socket = descriptor["socket"]
        .as_str()
        .ok_or_else(|| failure("missing notification socket"))?;
    let local = LocalSocket::bind().map_err(failure)?;
    local
        .socket
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(failure)?;
    local
        .socket
        .set_write_timeout(Some(Duration::from_millis(500)))
        .map_err(failure)?;
    local.socket.connect(socket).map_err(failure)?;
    let frame = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string();
    if frame.len() > MAX_FRAME {
        return Err(failure("oversized notification"));
    }
    local.socket.send(frame.as_bytes()).map_err(failure)?;
    let mut reply = [0; MAX_FRAME + 1];
    let length = local.socket.recv(&mut reply).map_err(failure)?;
    let reply: Value = serde_json::from_slice(&reply[..length]).map_err(failure)?;
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
    use std::time::Instant;

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
    fn structured_activation_is_durable_bounded_and_fenced_by_runner_authority() {
        let directory = LocalSocket::bind().unwrap();
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
