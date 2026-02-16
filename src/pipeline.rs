//! The self-feeding async pipeline.
//!
//! This is the heart of rust-pipeline. Messages flow through stages
//! connected by tokio mpsc channels. Each stage runs as an independent
//! task, giving us true concurrency with natural backpressure.
//!
//! ```text
//! [Ingress] →tx→ [Parse] →tx→ [Validate] →tx→ [Route+Dispatch] →tx→ [Reinject]
//!      ↑                                                                  |
//!      └──────────── serialize to raw bytes (UNTRUSTED) ──────────────────┘
//! ```
//!
//! Handler responses are serialized back to raw bytes and re-injected
//! at the ingress — losing all trust, going through the full validation
//! gauntlet again. The pipeline IS the trust boundary.

use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use crate::envelope::{build_envelope, parse_envelope, AgentId, Envelope};
use crate::error::PipelineError;
use crate::handler::{HandlerContext, HandlerResponse, ValidatedPayload};
use crate::registry::ListenerRegistry;
use crate::thread::ThreadRegistry;

/// Channel buffer size for inter-stage communication.
const CHANNEL_BUFFER: usize = 256;

/// The self-feeding pipeline.
///
/// Owns the registry, thread state, and manages the async task stages.
/// Messages enter via `inject()`, flow through validation/routing/dispatch,
/// and handler responses re-enter as untrusted raw bytes.
pub struct Pipeline {
    /// Ingress channel — external messages and re-injected responses enter here.
    ingress_tx: mpsc::Sender<Vec<u8>>,

    /// Listener/handler registry (shared across stages).
    registry: Arc<ListenerRegistry>,

    /// Thread registry (shared, behind a mutex for safe mutation).
    threads: Arc<Mutex<ThreadRegistry>>,

    /// Shutdown signal.
    shutdown_tx: Option<mpsc::Sender<()>>,

    /// Join handles for pipeline tasks.
    handles: Vec<tokio::task::JoinHandle<()>>,
}

// ── Internal stage message types ─────────────────────────────────────

/// Message after successful envelope parsing.
struct ParsedMsg {
    envelope: Envelope,
}

/// Message after successful validation.
struct ValidatedMsg {
    envelope: Envelope,
    payload: ValidatedPayload,
}

/// Message after successful routing.
struct RoutedMsg {
    envelope: Envelope,
    payload: ValidatedPayload,
    target_name: AgentId,
}

impl Pipeline {
    /// Create a new pipeline with the given registry and thread state.
    ///
    /// Call `run()` to start the pipeline tasks, then `inject()` to
    /// feed messages into it.
    pub fn new(registry: ListenerRegistry, threads: ThreadRegistry) -> Self {
        let (ingress_tx, _) = mpsc::channel(CHANNEL_BUFFER);

        Self {
            ingress_tx,
            registry: Arc::new(registry),
            threads: Arc::new(Mutex::new(threads)),
            shutdown_tx: None,
            handles: Vec::new(),
        }
    }

    /// Start the pipeline stages as tokio tasks.
    ///
    /// This spawns the stage tasks and wires them together with channels.
    /// The pipeline runs until `shutdown()` is called.
    pub fn run(&mut self) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Create inter-stage channels
        let (ingress_tx, ingress_rx) = mpsc::channel::<Vec<u8>>(CHANNEL_BUFFER);
        let (parsed_tx, parsed_rx) = mpsc::channel::<ParsedMsg>(CHANNEL_BUFFER);
        let (validated_tx, validated_rx) = mpsc::channel::<ValidatedMsg>(CHANNEL_BUFFER);
        let (routed_tx, routed_rx) = mpsc::channel::<RoutedMsg>(CHANNEL_BUFFER);

        // Store ingress_tx for injection
        self.ingress_tx = ingress_tx.clone();

        // Clone shared state for each stage
        let registry = self.registry.clone();
        let registry2 = self.registry.clone();
        let registry3 = self.registry.clone();
        let threads = self.threads.clone();
        let threads2 = self.threads.clone();

        // Re-injection channel: dispatch sends raw bytes back to ingress
        let reinject_tx = ingress_tx.clone();

        // ── Stage 1: Parse ───────────────────────────────────────────
        // Raw bytes → Envelope (or error)
        let h1 = tokio::spawn(parse_stage(ingress_rx, parsed_tx, shutdown_rx));

        // ── Stage 2: Validate ────────────────────────────────────────
        // Envelope → ValidatedMsg (XSD check on payload)
        let h2 = tokio::spawn(validate_stage(parsed_rx, validated_tx, registry));

        // ── Stage 3: Route ───────────────────────────────────────────
        // ValidatedMsg → RoutedMsg (resolve target, enforce peers)
        let h3 = tokio::spawn(route_stage(
            validated_rx,
            routed_tx,
            registry2,
            threads,
        ));

        // ── Stage 4: Dispatch + Reinject ─────────────────────────────
        // RoutedMsg → call handler → serialize response → reinject
        let h4 = tokio::spawn(dispatch_stage(
            routed_rx,
            reinject_tx,
            registry3,
            threads2,
        ));

        self.handles = vec![h1, h2, h3, h4];

        info!("pipeline started (4 stages)");
    }

    /// Inject raw bytes into the pipeline.
    ///
    /// This is the external API for feeding messages. The bytes
    /// are untrusted and will go through the full validation gauntlet.
    pub async fn inject(&self, raw: Vec<u8>) -> Result<(), PipelineError> {
        self.ingress_tx
            .send(raw)
            .await
            .map_err(|_| PipelineError::Handler("pipeline shut down".into()))
    }

    /// Graceful shutdown — signal all stages to stop.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        for handle in self.handles.drain(..) {
            let _ = handle.await;
        }
        info!("pipeline shut down");
    }

    /// Get a reference to the listener registry.
    pub fn registry(&self) -> &ListenerRegistry {
        &self.registry
    }

    /// Get a clone of the thread registry handle.
    pub fn threads(&self) -> Arc<Mutex<ThreadRegistry>> {
        self.threads.clone()
    }

    /// Get a clone of the ingress sender (for external use).
    pub fn ingress_tx(&self) -> mpsc::Sender<Vec<u8>> {
        self.ingress_tx.clone()
    }
}

// ── Stage implementations ────────────────────────────────────────────

/// Stage 1: Parse raw bytes into envelopes.
async fn parse_stage(
    mut rx: mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<ParsedMsg>,
    mut shutdown: mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {
            Some(raw) = rx.recv() => {
                match parse_envelope(&raw) {
                    Ok(envelope) => {
                        debug!(
                            from = %envelope.meta.from,
                            to = ?envelope.meta.to,
                            thread = %envelope.meta.thread,
                            payload = %envelope.payload_tag,
                            "parsed envelope"
                        );
                        if tx.send(ParsedMsg { envelope }).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("parse failed: {e}");
                        // Message dies here — it failed the first trust gate
                    }
                }
            }
            _ = shutdown.recv() => {
                info!("parse stage shutting down");
                break;
            }
        }
    }
}

/// Stage 2: Validate payload against schema.
async fn validate_stage(
    mut rx: mpsc::Receiver<ParsedMsg>,
    tx: mpsc::Sender<ValidatedMsg>,
    registry: Arc<ListenerRegistry>,
) {
    while let Some(msg) = rx.recv().await {
        let tag = &msg.envelope.payload_tag;
        let payload_xml = &msg.envelope.payload_raw;

        match registry.schemas.validate(tag, payload_xml) {
            Ok(()) => {
                debug!(tag = %tag, "payload validated");
                let validated = ValidatedMsg {
                    payload: ValidatedPayload {
                        xml: msg.envelope.payload_raw.clone(),
                        tag: msg.envelope.payload_tag.clone(),
                    },
                    envelope: msg.envelope,
                };
                if tx.send(validated).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                warn!(tag = %tag, "validation failed: {e}");
                // Message dies — failed the second trust gate
            }
        }
    }
}

/// Stage 3: Resolve routing and enforce peer constraints.
async fn route_stage(
    mut rx: mpsc::Receiver<ValidatedMsg>,
    tx: mpsc::Sender<RoutedMsg>,
    registry: Arc<ListenerRegistry>,
    threads: Arc<Mutex<ThreadRegistry>>,
) {
    while let Some(msg) = rx.recv().await {
        let to = msg.envelope.meta.to.as_deref();
        let tag = &msg.envelope.payload_tag;

        // Resolve route
        let entries = match registry.routing.resolve(to, tag) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("routing failed: {e}");
                continue; // Message dies — no route
            }
        };

        // For now, take the first match (no broadcast in Phase 1)
        let target = &entries[0];

        // Enforce peer constraints
        if let Err(e) = registry
            .routing
            .enforce_peers(&msg.envelope.meta.from, &target.name)
        {
            warn!("{e}");
            continue; // Message dies — peer violation
        }

        // Register thread if needed
        {
            let mut threads = threads.lock().await;
            let thread_id = &msg.envelope.meta.thread;
            if threads.lookup(thread_id).is_none() {
                threads.register_thread(
                    thread_id,
                    &msg.envelope.meta.from,
                    &target.name,
                );
            }
        }

        debug!(
            target_name = %target.name,
            from = %msg.envelope.meta.from,
            "routed"
        );

        let routed = RoutedMsg {
            envelope: msg.envelope,
            payload: msg.payload,
            target_name: target.name.clone(),
        };

        if tx.send(routed).await.is_err() {
            break;
        }
    }
}

/// Stage 4: Dispatch to handler and reinject response.
async fn dispatch_stage(
    mut rx: mpsc::Receiver<RoutedMsg>,
    reinject_tx: mpsc::Sender<Vec<u8>>,
    registry: Arc<ListenerRegistry>,
    threads: Arc<Mutex<ThreadRegistry>>,
) {
    while let Some(msg) = rx.recv().await {
        let handler = match registry.get_handler(&msg.target_name) {
            Some(h) => h,
            None => {
                error!(target = %msg.target_name, "handler not found (registry inconsistency)");
                continue;
            }
        };

        let ctx = HandlerContext {
            thread_id: msg.envelope.meta.thread.clone(),
            from: msg.envelope.meta.from.clone(),
            own_name: msg.target_name.clone(),
        };

        // Call handler
        let result = handler.handle(msg.payload, ctx).await;

        match result {
            Ok(HandlerResponse::None) => {
                debug!(handler = %msg.target_name, "handler returned None (terminal)");
                // Thread ends here — no re-injection
            }
            Ok(HandlerResponse::Reply { payload_xml }) => {
                // Prune chain and route back to caller
                let mut threads = threads.lock().await;
                match threads.prune_for_response(&msg.envelope.meta.thread) {
                    Some(prune) => {
                        debug!(
                            handler = %msg.target_name,
                            target = %prune.target,
                            "reply → pruned chain"
                        );

                        // Build envelope and serialize to raw bytes (UNTRUSTED)
                        match build_envelope(
                            &msg.target_name,
                            &prune.target,
                            &prune.thread_id,
                            &payload_xml,
                        ) {
                            Ok(raw) => {
                                if reinject_tx.send(raw).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("failed to build reply envelope: {e}");
                            }
                        }
                    }
                    None => {
                        debug!(
                            handler = %msg.target_name,
                            "chain exhausted — reply dropped"
                        );
                    }
                }
            }
            Ok(HandlerResponse::Send { to, payload_xml }) => {
                // Forward to named target — extend chain
                let new_thread = {
                    let mut threads = threads.lock().await;

                    // Enforce peer constraints
                    if let Err(e) = registry.routing.enforce_peers(&msg.target_name, &to) {
                        warn!("{e}");
                        continue;
                    }

                    threads.extend_chain(&msg.envelope.meta.thread, &to)
                };

                debug!(
                    handler = %msg.target_name,
                    to = %to,
                    "send → extended chain"
                );

                // Build envelope and serialize to raw bytes (UNTRUSTED)
                match build_envelope(&msg.target_name, &to, &new_thread, &payload_xml) {
                    Ok(raw) => {
                        if reinject_tx.send(raw).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("failed to build send envelope: {e}");
                    }
                }
            }
            Err(e) => {
                error!(handler = %msg.target_name, "handler error: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::FnHandler;

    /// Build a minimal test pipeline with an echo handler.
    fn setup_echo_pipeline() -> Pipeline {
        let mut registry = ListenerRegistry::new();
        let threads = ThreadRegistry::new();

        // Echo handler: replies with the same payload
        let echo = FnHandler(|p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::Reply {
                    payload_xml: p.xml,
                }) as Result<HandlerResponse, PipelineError>
            })
        });

        registry.register(
            "echo",
            "Greeting",
            echo,
            false,
            vec![],
            "Echo handler",
            None,
        );

        Pipeline::new(registry, threads)
    }

    #[tokio::test]
    async fn pipeline_creates() {
        let pipeline = setup_echo_pipeline();
        assert!(pipeline.registry().has_listener("echo"));
    }

    #[tokio::test]
    async fn pipeline_starts_and_stops() {
        let mut pipeline = setup_echo_pipeline();
        pipeline.run();

        // Give stages time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn inject_and_process() {
        let mut registry = ListenerRegistry::new();
        let mut threads = ThreadRegistry::new();
        threads.initialize_root("test");

        // Sink handler: receives messages and records them
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();

        let sink = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let r = received_clone.clone();
            Box::pin(async move {
                let text = String::from_utf8_lossy(&p.xml).to_string();
                r.lock().await.push(text);
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        registry.register("sink", "Greeting", sink, false, vec![], "Sink", None);

        let mut pipeline = Pipeline::new(registry, threads);
        pipeline.run();

        // Inject a message
        let envelope = build_envelope(
            "test-sender",
            "sink",
            "test-thread-001",
            b"<Greeting><text>hello</text></Greeting>",
        )
        .unwrap();

        pipeline.inject(envelope).await.unwrap();

        // Wait for processing
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains("hello"));

        pipeline.shutdown().await;
    }
}
