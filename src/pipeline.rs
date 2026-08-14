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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use crate::codec::{decode_envelope, encode_envelope};
use crate::envelope::AgentId;
use crate::error::PipelineError;
use crate::federation::FederationEgress;
use crate::handler::{HandlerContext, HandlerResponse, ValidatedPayload};
use crate::middleware::{DispatchMeta, Middleware, PostDispatchVerdict, PreDispatchVerdict};
use crate::registry::ListenerRegistry;
use crate::thread::ThreadRegistry;
use crate::wire::{Address, Envelope, Meta, Payload, PayloadValue};

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

    /// Middleware chain — run before/after handler dispatch.
    middleware: Vec<Arc<dyn Middleware>>,

    /// Shutdown signal.
    shutdown_tx: Option<mpsc::Sender<()>>,

    /// Join handles for pipeline tasks.
    handles: Vec<tokio::task::JoinHandle<()>>,

    /// Optional federation egress — remote-node destinations escalate here.
    federation_egress: Option<FederationEgress>,

    /// Optional observer for in-flight call edges (host supervision / ledger hook).
    dispatch_observer: Option<Arc<dyn DispatchObserver>>,
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

/// Shared state every dispatch worker needs, bundled so it threads through the worker
/// fan-out as a single cheaply-cloned handle instead of a long argument list.
struct DispatchCtx {
    registry: Arc<ListenerRegistry>,
    threads: Arc<Mutex<ThreadRegistry>>,
    reinject_tx: mpsc::Sender<Vec<u8>>,
    middleware: Arc<Vec<Arc<dyn Middleware>>>,
    dispatch_observer: Option<Arc<dyn DispatchObserver>>,
}

/// Per-delivery-thread serial workers, keyed by thread id (= instance). The dispatch
/// stage inserts a worker on first sight of a thread; the worker self-reaps after an idle
/// period. All map mutation is done under the `Mutex`, which makes enqueue (dispatch) and
/// reap (worker) mutually exclusive — the invariant that keeps exactly one worker alive
/// per key and loses no message.
type WorkerMap = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<RoutedMsg>>>>;

/// How long a per-thread worker waits idle before reaping itself.
const WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether an in-flight call edge is a synchronous sub-call or an async delegation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    /// A [`HandlerResponse::Send`] sub-call: the caller is **suspended** on the chain until
    /// the callee replies.
    Sync,
    /// A [`HandlerResponse::Spawn`] delegation: the caller was **acked and continues**; the
    /// result returns later.
    Async,
}

/// Observes the in-flight **call-edge** lifecycle so a host can supervise sub-calls.
///
/// The pipeline is the only layer that sees a handler open a call edge (`Send`/`Spawn`
/// extends the chain) and later see it close (the callee's response prunes back), so it
/// emits those events; the **host decides what to do with them** — apply deadlines/timeouts
/// (reliability is host policy above the wire) and, for async edges, maintain the durable
/// in-flight ledger that gates eviction (a caller with an open async edge may be
/// tiered/evicted but must remain restorable — not garbage-collected — until it closes).
///
/// This is what lets the host time out **both** a hung synchronous `Send` (the caller is
/// suspended, waiting for a reply that never comes) and a hung async `Spawn` — the pipeline
/// informs the host that a sub-call is running; the host sets the deadline and, on expiry,
/// recovers (e.g. injects a failure back to the caller).
///
/// `on_open`/`on_close` are synchronous and must be cheap (they run inline in dispatch); a
/// host needing async/durable bookkeeping should hand off to a channel or its journal.
pub trait DispatchObserver: Send + Sync + 'static {
    /// A call edge opened: `caller` dispatched to `callee`, and a response is expected on
    /// `thread`. For [`CallKind::Async`] this is an obligation (don't GC the caller until it
    /// closes); for [`CallKind::Sync`] the caller is suspended awaiting it.
    fn on_open(&self, caller: &str, callee: &str, thread: &str, kind: CallKind);

    /// The call edge identified by `thread` closed — the callee produced its response and
    /// the chain pruned back. Threads that were never opened (e.g. external ingress) should
    /// be ignored.
    fn on_close(&self, thread: &str);
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
            middleware: Vec::new(),
            shutdown_tx: None,
            handles: Vec::new(),
            federation_egress: None,
            dispatch_observer: None,
        }
    }

    /// Install the federation-egress hook: messages addressed to a registered remote node
    /// are handed to the federation server instead of being routed locally. Call before
    /// `run()`.
    pub fn with_federation(&mut self, egress: FederationEgress) {
        self.federation_egress = Some(egress);
    }

    /// Install a [`DispatchObserver`] so a host can supervise in-flight call edges —
    /// deadlines/timeouts for `Send` and `Spawn`, and the async in-flight ledger. Call
    /// before `run()`.
    pub fn with_dispatch_observer(&mut self, observer: Arc<dyn DispatchObserver>) {
        self.dispatch_observer = Some(observer);
    }

    /// Add middleware to the dispatch chain.
    ///
    /// Middleware runs in registration order for pre-dispatch,
    /// and reverse order for post-dispatch (onion wrapping).
    /// Must be called before `run()`.
    pub fn add_middleware(&mut self, mw: impl Middleware) {
        self.middleware.push(Arc::new(mw));
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
            self.federation_egress.clone(),
        ));

        // ── Stage 4: Dispatch + Reinject ─────────────────────────────
        // RoutedMsg → call handler → serialize response → reinject
        let middleware: Arc<Vec<Arc<dyn Middleware>>> = Arc::new(self.middleware.drain(..).collect());
        let dispatch_ctx = Arc::new(DispatchCtx {
            registry: registry3,
            threads: threads2,
            reinject_tx,
            middleware,
            dispatch_observer: self.dispatch_observer.clone(),
        });
        let h4 = tokio::spawn(dispatch_stage(routed_rx, dispatch_ctx));

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
                match decode_envelope(&raw) {
                    Ok(envelope) => {
                        debug!(
                            from = %envelope.meta.from,
                            to = ?envelope.meta.to,
                            thread = %envelope.meta.thread,
                            payload = %envelope.payload.tag,
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
        let tag = msg.envelope.payload.tag.clone();

        match registry.schemas.validate_value(&tag, &msg.envelope.payload.value) {
            Ok(()) => {
                debug!(tag = %tag, "payload validated");
                let validated = ValidatedMsg {
                    payload: ValidatedPayload {
                        tag,
                        value: msg.envelope.payload.value.clone(),
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
    federation: Option<FederationEgress>,
) {
    while let Some(msg) = rx.recv().await {
        // Federation egress: a destination whose leading segment is a registered remote
        // node leaves via the federation server, not local routing (resolve-or-escalate).
        if let Some(fed) = &federation {
            let is_remote = msg
                .envelope
                .meta
                .to
                .as_ref()
                .and_then(|a| a.segments.first())
                .map(|s| fed.directory.is_remote(&s.name))
                .unwrap_or(false);
            if is_remote {
                debug!(to = ?msg.envelope.meta.to, "federation egress → remote node");
                if fed.tx.send(msg.envelope).await.is_err() {
                    warn!("federation egress channel closed");
                }
                continue;
            }
        }

        // Route to the organism segment — the listener. (Namespace/key/buffer are
        // instance-level concerns resolved above the pipeline; the listener is shared.)
        let to = msg.envelope.meta.to.as_ref().and_then(|a| a.organism());
        let tag = &msg.envelope.payload.tag;
        // `from` for routing/peer/thread purposes is the sender's organism (listener).
        let from_name = msg.envelope.meta.from.organism().unwrap_or_default().to_string();

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
        if let Err(e) = registry.routing.enforce_peers(&from_name, &target.name) {
            warn!("{e}");
            continue; // Message dies — peer violation
        }

        // Register thread if needed
        {
            let mut threads = threads.lock().await;
            let thread_id = &msg.envelope.meta.thread;
            if threads.lookup(thread_id).is_none() {
                threads.register_thread(thread_id, &from_name, &target.name);
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

/// Stage 4: Dispatch — fan out to per-thread serial workers.
///
/// The dispatcher itself does no handler work; it routes each [`RoutedMsg`] to the worker
/// for its delivery thread (spawning one on first sight), so that:
///
/// - **serial per thread (= per instance):** messages sharing a thread id go to the same
///   worker's ordered queue and run one-at-a-time, in arrival order. This is the
///   `bob[alice]` invariant — a handler is never called concurrently for the same
///   instance, so its `&self` / checkpoint state is safe.
/// - **concurrent across threads (= across users):** different thread ids have independent
///   workers, so `bob[alice]` and `bob[carol]` run in parallel; a slow handler no longer
///   head-of-line-blocks the whole pipeline.
///
/// Enqueue below and reap in [`serial_worker`] both mutate the map only while holding its
/// `Mutex`, and the per-worker send is synchronous (unbounded) — so they are mutually
/// exclusive. That is what guarantees exactly one live worker per key and no lost message.
async fn dispatch_stage(mut rx: mpsc::Receiver<RoutedMsg>, ctx: Arc<DispatchCtx>) {
    let workers: WorkerMap = Arc::new(Mutex::new(HashMap::new()));

    while let Some(msg) = rx.recv().await {
        let key = msg.envelope.meta.thread.clone();
        let mut pending = Some(msg);

        // Hand off to the thread's worker. The retry loop only re-runs in the (normally
        // unreachable) case that a worker's channel was found closed under the lock.
        while let Some(m) = pending.take() {
            let mut map = workers.lock().await;
            let tx = match map.get(&key) {
                Some(tx) => tx.clone(),
                None => {
                    let (wtx, wrx) = mpsc::unbounded_channel();
                    tokio::spawn(serial_worker(key.clone(), wrx, workers.clone(), ctx.clone()));
                    map.insert(key.clone(), wtx.clone());
                    wtx
                }
            };
            // Synchronous unbounded send while still holding the map lock, so this can't
            // interleave with a worker reaping itself (which also takes the lock).
            match tx.send(m) {
                Ok(()) => {}
                Err(mpsc::error::SendError(returned)) => {
                    map.remove(&key);
                    pending = Some(returned);
                }
            }
        }
    }
    // `rx` closed → pipeline shutting down. Dropping `workers` here releases this task's
    // senders; each idle worker times out and reaps, and the runtime reclaims them.
}

/// A per-delivery-thread serial worker: processes its queue one message at a time (so
/// same-thread dispatch is serial and ordered), and reaps itself after
/// [`WORKER_IDLE_TIMEOUT`] of inactivity so the [`WorkerMap`] stays bounded.
async fn serial_worker(
    key: String,
    mut wrx: mpsc::UnboundedReceiver<RoutedMsg>,
    workers: WorkerMap,
    ctx: Arc<DispatchCtx>,
) {
    loop {
        match tokio::time::timeout(WORKER_IDLE_TIMEOUT, wrx.recv()).await {
            Ok(Some(msg)) => {
                process_message(msg, &ctx).await;
            }
            Ok(None) => break, // all senders dropped — pipeline shutdown
            Err(_) => {
                // Idle. Reap under the map lock. Re-check the queue under that lock: the
                // dispatcher enqueues under the same lock, so if a message landed after the
                // timeout fired we see it here and keep going instead of dropping it.
                let mut map = workers.lock().await;
                match wrx.try_recv() {
                    Err(mpsc::error::TryRecvError::Empty)
                    | Err(mpsc::error::TryRecvError::Disconnected) => {
                        map.remove(&key);
                        break;
                    }
                    Ok(msg) => {
                        drop(map);
                        process_message(msg, &ctx).await;
                    }
                }
            }
        }
    }
}

/// Process one routed message: pre-dispatch middleware → handler → post-dispatch
/// middleware → response handling → reinject. Runs inside a [`serial_worker`], so calls
/// for one thread are serialized while calls across threads run concurrently.
///
/// Thread-registry mutations take the lock only for the mutation itself; the guard is
/// dropped **before** the `reinject_tx.send().await` so a slow/full reinject channel can't
/// serialize concurrent workers on the thread lock.
async fn process_message(msg: RoutedMsg, ctx: &DispatchCtx) {
    let DispatchCtx {
        registry,
        threads,
        reinject_tx,
        middleware,
        dispatch_observer,
    } = ctx;

    let handler = match registry.get_handler(&msg.target_name) {
        Some(h) => h,
        None => {
            error!(target = %msg.target_name, "handler not found (registry inconsistency)");
            return;
        }
    };

    let ctx = HandlerContext {
        thread_id: msg.envelope.meta.thread.clone(),
        from: msg.envelope.meta.from.clone(),
        own_name: Address::flat(&msg.target_name),
        provenance: msg.envelope.meta.provenance,
    };

    // Build dispatch metadata for middleware (name-keyed)
    let meta = DispatchMeta {
        from: msg.envelope.meta.from.organism().unwrap_or_default().to_string(),
        to: msg.target_name.clone(),
        thread_id: msg.envelope.meta.thread.clone(),
        payload_tag: msg.payload.tag.clone(),
    };

    // Pre-dispatch middleware chain (in registration order).
    // Payload may be transformed in-flight by middleware.
    let mut payload = msg.payload.clone();
    let mut short_circuited = None;
    for mw in middleware.iter() {
        match mw.pre_dispatch(&meta, &payload).await {
            Ok(PreDispatchVerdict::Continue) => {}
            Ok(PreDispatchVerdict::Transform(new_payload)) => {
                debug!(handler = %msg.target_name, "middleware transformed payload");
                payload = new_payload;
            }
            Ok(PreDispatchVerdict::ShortCircuit(response)) => {
                debug!(handler = %msg.target_name, "middleware short-circuited dispatch");
                short_circuited = Some(Ok(response));
                break;
            }
            Err(e) => {
                short_circuited = Some(Err(e));
                break;
            }
        }
    }

    // Call handler (unless short-circuited)
    let result = if let Some(r) = short_circuited {
        r
    } else {
        handler.handle(payload.clone(), ctx).await
    };

    // Post-dispatch middleware chain (in reverse order).
    let result = match result {
        Ok(response) => {
            let mut current = Some(response);
            for mw in middleware.iter().rev() {
                let r = current.take().expect("post-dispatch: response consumed");
                match mw.post_dispatch(&meta, &payload, r).await {
                    Ok(PostDispatchVerdict::PassThrough(r)) => current = Some(r),
                    Ok(PostDispatchVerdict::Replace(r)) => {
                        debug!(handler = %msg.target_name, "middleware replaced response");
                        current = Some(r);
                    }
                    Err(e) => {
                        error!(handler = %msg.target_name, "middleware post-dispatch error: {e}");
                        // On middleware error, fall through to dispatch error
                        current = None;
                        break;
                    }
                }
            }
            match current {
                Some(r) => Ok(r),
                None => Err(PipelineError::Handler("middleware post-dispatch error".into())),
            }
        }
        Err(e) => Err(e),
    };

    // Provenance is CARRIED from the inbound envelope into whatever the dispatcher builds
    // (Provenance is Copy). In Commit 1 nothing stamps new bits, so the union is just
    // propagation; §step-4 proves it accumulates across a multi-hop chain.
    let inbound_prov = msg.envelope.meta.provenance;

    match result {
        Ok(HandlerResponse::None) => {
            // The callee finished with nothing to say — its call edge (if any) closes.
            if let Some(obs) = dispatch_observer {
                obs.on_close(&msg.envelope.meta.thread);
            }
            // Synthesize ACK if a parent exists in the thread chain. Take the lock only to
            // prune, then release it before building/reinjecting.
            let prune = {
                let mut threads = threads.lock().await;
                threads.prune_for_response(&msg.envelope.meta.thread)
            };
            match prune {
                Some(prune) => {
                    debug!(handler = %msg.target_name, target = %prune.target, "None → synthesized ACK for parent");
                    let ack = Payload::new(
                        "ToolResponse",
                        PayloadValue::record([
                            ("success", PayloadValue::Boolean(true)),
                            ("result", PayloadValue::text("ack")),
                        ]),
                    );
                    let env = Envelope {
                        meta: Meta {
                            from: Address::flat(&msg.target_name),
                            to: Some(Address::flat(&prune.target)),
                            thread: prune.thread_id.clone(),
                            provenance: inbound_prov,
                        },
                        payload: ack,
                    };
                    match encode_envelope(&env) {
                        Ok(raw) => {
                            let _ = reinject_tx.send(raw).await;
                        }
                        Err(e) => error!("failed to build ACK envelope: {e}"),
                    }
                }
                None => debug!(handler = %msg.target_name, "handler returned None (terminal, no parent)"),
            }
        }
        Ok(HandlerResponse::Reply { payload }) => {
            // The callee on this thread produced its response — any call edge awaiting on
            // this thread is now closed (the host clears its deadline / discharges it).
            if let Some(obs) = dispatch_observer {
                obs.on_close(&msg.envelope.meta.thread);
            }
            let prune = {
                let mut threads = threads.lock().await;
                threads.prune_for_response(&msg.envelope.meta.thread)
            };
            match prune {
                Some(prune) => {
                    debug!(handler = %msg.target_name, target = %prune.target, "reply → pruned chain");
                    // Build envelope and serialize to raw bytes (UNTRUSTED)
                    let env = Envelope {
                        meta: Meta {
                            from: Address::flat(&msg.target_name),
                            to: Some(Address::flat(&prune.target)),
                            thread: prune.thread_id.clone(),
                            provenance: inbound_prov,
                        },
                        payload,
                    };
                    match encode_envelope(&env) {
                        Ok(raw) => {
                            let _ = reinject_tx.send(raw).await;
                        }
                        Err(e) => error!("failed to build reply envelope: {e}"),
                    }
                }
                None => debug!(handler = %msg.target_name, "chain exhausted — reply dropped"),
            }
        }
        Ok(HandlerResponse::Spawn { to, payload }) => {
            // Async delegation. Two things happen, in this order:
            //   1. Hand the work to the callee on a DETACHED chain that records THIS handler
            //      as the return target — so the callee's eventual Reply prunes back here
            //      through the ordinary reply path (the callee never learns who called it).
            //   2. Immediately ack THIS handler on its own thread, so it resumes and can
            //      respond to its own caller ("received ≠ done").
            // The ack is sent only AFTER a successful, peer-checked handoff, so an
            // unroutable spawn acks `accepted: false` rather than a false "working on it".
            let to_name = to.organism().unwrap_or_default().to_string();
            let accepted = match registry.routing.enforce_peers(&msg.target_name, &to_name) {
                Ok(()) => {
                    let obligation_thread = {
                        let mut threads = threads.lock().await;
                        threads.extend_chain(&msg.envelope.meta.thread, &to_name)
                    };
                    let work = Envelope {
                        meta: Meta {
                            from: Address::flat(&msg.target_name),
                            to: Some(to),
                            thread: obligation_thread.clone(),
                            provenance: inbound_prov,
                        },
                        payload,
                    };
                    match encode_envelope(&work) {
                        Ok(raw) => {
                            let _ = reinject_tx.send(raw).await;
                            debug!(handler = %msg.target_name, to = %to_name, thread = %obligation_thread, "spawn → detached delegation");
                            // Async call edge opened — surface it for the host's in-flight
                            // ledger + deadline.
                            if let Some(obs) = dispatch_observer {
                                obs.on_open(&msg.target_name, &to_name, &obligation_thread, CallKind::Async);
                            }
                            true
                        }
                        Err(e) => {
                            error!("failed to build spawn envelope: {e}");
                            false
                        }
                    }
                }
                Err(e) => {
                    warn!("spawn rejected (peer): {e}");
                    false
                }
            };

            // Ack the caller on its own thread so it can proceed regardless of outcome.
            let ack = Payload::new(
                "SpawnAck",
                PayloadValue::record([("accepted", PayloadValue::Boolean(accepted))]),
            );
            let ack_env = Envelope {
                meta: Meta {
                    from: Address::flat(&to_name),
                    to: Some(Address::flat(&msg.target_name)),
                    thread: msg.envelope.meta.thread.clone(),
                    provenance: inbound_prov,
                },
                payload: ack,
            };
            match encode_envelope(&ack_env) {
                Ok(raw) => {
                    let _ = reinject_tx.send(raw).await;
                }
                Err(e) => error!("failed to build spawn-ack envelope: {e}"),
            }
        }
        Ok(HandlerResponse::Send { to, payload }) => {
            // Forward to a target — extend chain (route by organism = listener).
            // Peer enforcement uses the routing table only, so it needs no thread lock.
            let to_name = to.organism().unwrap_or_default().to_string();
            if let Err(e) = registry.routing.enforce_peers(&msg.target_name, &to_name) {
                warn!("{e}");
                return;
            }
            let new_thread = {
                let mut threads = threads.lock().await;
                threads.extend_chain(&msg.envelope.meta.thread, &to_name)
            };
            debug!(handler = %msg.target_name, to = %to, "send → extended chain (sync sub-call)");
            // Build envelope and serialize to raw bytes (UNTRUSTED)
            let env = Envelope {
                meta: Meta {
                    from: Address::flat(&msg.target_name),
                    to: Some(to),
                    thread: new_thread.clone(),
                    provenance: inbound_prov,
                },
                payload,
            };
            match encode_envelope(&env) {
                Ok(raw) => {
                    let _ = reinject_tx.send(raw).await;
                    // A synchronous call edge opened: the caller is now suspended awaiting
                    // the reply on `new_thread`. Surface it so the host can apply a deadline
                    // (and time out a hung sub-call).
                    if let Some(obs) = dispatch_observer {
                        obs.on_open(&msg.target_name, &to_name, &new_thread, CallKind::Sync);
                    }
                }
                Err(e) => error!("failed to build send envelope: {e}"),
            }
        }
        Err(e) => error!(handler = %msg.target_name, "handler error: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::handler::FnHandler;
    use crate::wire::Provenance;

    /// Build inbound wire bytes for a message (the typed replacement for the old
    /// `build_envelope` test helper).
    fn inbound(from: &str, to: &str, thread: &str, payload: Payload) -> Vec<u8> {
        encode_envelope(&Envelope {
            meta: Meta {
                from: from.into(),
                to: Some(to.into()),
                thread: thread.into(),
                provenance: Provenance::EMPTY,
            },
            payload,
        })
        .unwrap()
    }

    /// The text of a payload's `text` field, or "" — the common assertion target.
    fn text_of(p: &ValidatedPayload) -> String {
        p.value
            .get("text")
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string()
    }

    /// Build a minimal test pipeline with an echo handler.
    fn setup_echo_pipeline() -> Pipeline {
        let mut registry = ListenerRegistry::new();
        let threads = ThreadRegistry::new();

        // Echo handler: replies with the same payload
        let echo = FnHandler(|p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::Reply {
                    payload: p.to_payload(),
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
    async fn none_with_parent_synthesizes_ack() {
        // Parent handler forwards to child, child returns None.
        // Pipeline should synthesize ACK back to parent.
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_clone = call_count.clone();

        // Parent: first call → Send to child; second call → record ACK
        let parent = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let r = received_clone.clone();
            let cc = call_count_clone.clone();
            Box::pin(async move {
                let mut count = cc.lock().await;
                *count += 1;
                if *count == 1 {
                    // First call: forward to child
                    Ok(HandlerResponse::Send {
                        to: "child".into(),
                        payload: Payload::single("ChildRequest", "data", "go"),
                    })
                } else {
                    // Subsequent call: record what we received (should be ACK)
                    let result = p
                        .value
                        .get("result")
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string();
                    r.lock().await.push(format!("{}:{}", p.tag, result));
                    Ok(HandlerResponse::None)
                }
            })
        });

        // Child: always returns None
        let child = FnHandler(|_p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("parent", "ParentRequest", parent, false, vec!["child".into()], "Parent", None);
        // Register ToolResponse route so ACK replies route back to parent
        registry.routing.register("parent", "ToolResponse", false, vec!["child".into()], "Parent");
        registry.register("child", "ChildRequest", child, false, vec![], "Child", None);

        let threads = ThreadRegistry::new();
        let mut pipeline = Pipeline::new(registry, threads);
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "parent",
                "thread-ack-1",
                Payload::single("ParentRequest", "task", "do it"),
            ))
            .await
            .unwrap();

        // Wait for processing (parent→child→ACK→parent)
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1, "parent should receive exactly one ACK");
        assert!(msgs[0].contains("ToolResponse"), "ACK should be a ToolResponse");
        assert!(msgs[0].contains("ack"), "ACK should contain 'ack'");

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn none_at_root_stays_silent() {
        // Handler returns None with no parent — thread ends silently
        let mut registry = ListenerRegistry::new();
        let threads = ThreadRegistry::new();

        let sink = FnHandler(|_p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        registry.register("sink", "SinkRequest", sink, false, vec![], "Sink", None);

        let mut pipeline = Pipeline::new(registry, threads);
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "sink",
                "thread-silent-1",
                Payload::single("SinkRequest", "data", "gone"),
            ))
            .await
            .unwrap();

        // Wait — should not crash or hang
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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
                r.lock().await.push(text_of(&p));
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        registry.register("sink", "Greeting", sink, false, vec![], "Sink", None);

        let mut pipeline = Pipeline::new(registry, threads);
        pipeline.run();

        // Inject a message
        pipeline
            .inject(inbound(
                "test-sender",
                "sink",
                "test-thread-001",
                Payload::single("Greeting", "text", "hello"),
            ))
            .await
            .unwrap();

        // Wait for processing
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains("hello"));

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn middleware_pre_dispatch_short_circuit() {
        // Middleware short-circuits: handler should NOT be called.
        let handler_called = Arc::new(Mutex::new(false));
        let handler_called_clone = handler_called.clone();

        let sink = FnHandler(move |_p: ValidatedPayload, _ctx: HandlerContext| {
            let called = handler_called_clone.clone();
            Box::pin(async move {
                *called.lock().await = true;
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("sink", "Greeting", sink, false, vec![], "Sink", None);
        let threads = ThreadRegistry::new();

        let mut pipeline = Pipeline::new(registry, threads);

        // Short-circuit middleware: always blocks
        struct BlockAll;
        #[async_trait]
        impl Middleware for BlockAll {
            async fn pre_dispatch(
                &self,
                _meta: &DispatchMeta,
                _payload: &ValidatedPayload,
            ) -> Result<PreDispatchVerdict, PipelineError> {
                Ok(PreDispatchVerdict::ShortCircuit(HandlerResponse::None))
            }
        }
        pipeline.add_middleware(BlockAll);
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "sink",
                "thread-mw-1",
                Payload::single("Greeting", "text", "hi"),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(!*handler_called.lock().await, "handler should not be called when middleware short-circuits");

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn middleware_post_dispatch_replace() {
        // Handler returns None, middleware replaces with Reply.
        // We verify the replacement by having a parent receive the reply.
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_clone = call_count.clone();

        // Parent: first call → Send to child; second call → record response
        let parent = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let r = received_clone.clone();
            let cc = call_count_clone.clone();
            Box::pin(async move {
                let mut count = cc.lock().await;
                *count += 1;
                if *count == 1 {
                    Ok(HandlerResponse::Send {
                        to: "child".into(),
                        payload: Payload::single("ChildRequest", "data", "go"),
                    })
                } else {
                    let result = p
                        .value
                        .get("result")
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string();
                    r.lock().await.push(result);
                    Ok(HandlerResponse::None)
                }
            })
        });

        // Child: returns None (middleware will replace with Reply)
        let child = FnHandler(|_p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register(
            "parent", "ParentRequest", parent, false, vec!["child".into()], "Parent", None,
        );
        registry.routing.register("parent", "ToolResponse", false, vec!["child".into()], "Parent");
        registry.register("child", "ChildRequest", child, false, vec![], "Child", None);

        let threads = ThreadRegistry::new();
        let mut pipeline = Pipeline::new(registry, threads);

        // Middleware that replaces None with Reply for child handler
        struct ReplaceNone;
        #[async_trait]
        impl Middleware for ReplaceNone {
            async fn post_dispatch(
                &self,
                meta: &DispatchMeta,
                _payload: &ValidatedPayload,
                response: HandlerResponse,
            ) -> Result<PostDispatchVerdict, PipelineError> {
                if meta.to == "child" && matches!(response, HandlerResponse::None) {
                    return Ok(PostDispatchVerdict::Replace(HandlerResponse::Reply {
                        payload: Payload::new(
                            "ToolResponse",
                            PayloadValue::record([
                                ("success", PayloadValue::Boolean(true)),
                                ("result", PayloadValue::text("replaced")),
                            ]),
                        ),
                    }));
                }
                Ok(PostDispatchVerdict::PassThrough(response))
            }
        }
        pipeline.add_middleware(ReplaceNone);
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "parent",
                "thread-mw-2",
                Payload::single("ParentRequest", "task", "do it"),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1, "parent should receive middleware-replaced response");
        assert!(msgs[0].contains("replaced"), "response should contain middleware replacement");

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn middleware_pre_dispatch_transform() {
        // Middleware transforms the payload before the handler sees it.
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();

        let sink = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let r = received_clone.clone();
            Box::pin(async move {
                r.lock().await.push(text_of(&p));
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("sink", "Greeting", sink, false, vec![], "Sink", None);
        let threads = ThreadRegistry::new();

        let mut pipeline = Pipeline::new(registry, threads);

        // Middleware that marks the payload's text field
        struct Quarantine;
        #[async_trait]
        impl Middleware for Quarantine {
            async fn pre_dispatch(
                &self,
                _meta: &DispatchMeta,
                payload: &ValidatedPayload,
            ) -> Result<PreDispatchVerdict, PipelineError> {
                let orig = payload.value.get("text").and_then(|v| v.as_text()).unwrap_or("");
                let marked = PayloadValue::record([(
                    "text",
                    PayloadValue::text(format!("[quarantined] {orig}")),
                )]);
                Ok(PreDispatchVerdict::Transform(ValidatedPayload::new(
                    payload.tag.clone(),
                    marked,
                )))
            }
        }
        pipeline.add_middleware(Quarantine);
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "sink",
                "thread-mw-transform",
                Payload::single("Greeting", "text", "hello"),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains("[quarantined] hello"), "handler should see transformed payload, got: {}", msgs[0]);

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn empty_middleware_vec_no_effect() {
        // Pipeline with no middleware should work exactly as before.
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();

        let sink = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let r = received_clone.clone();
            Box::pin(async move {
                r.lock().await.push(text_of(&p));
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("sink", "Greeting", sink, false, vec![], "Sink", None);
        let threads = ThreadRegistry::new();

        let mut pipeline = Pipeline::new(registry, threads);
        // No middleware added — empty vec
        pipeline.run();

        pipeline
            .inject(inbound(
                "test-sender",
                "sink",
                "thread-mw-3",
                Payload::single("Greeting", "text", "works"),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let msgs = received.lock().await;
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains("works"));

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn dispatch_concurrent_across_threads() {
        // One listener ("bob"), two delivery threads ("alice", "carol"). Each handler call
        // holds for a beat; with per-thread workers they must overlap → max concurrency 2.
        use std::sync::atomic::{AtomicUsize, Ordering};

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let p = peak.clone();

        let bob = FnHandler(move |_p: ValidatedPayload, _ctx: HandlerContext| {
            let a = a.clone();
            let p = p.clone();
            Box::pin(async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("bob", "Greeting", bob, false, vec![], "Bob", None);
        let mut pipeline = Pipeline::new(registry, ThreadRegistry::new());
        pipeline.run();

        pipeline
            .inject(inbound("ext", "bob", "alice", Payload::single("Greeting", "text", "x")))
            .await
            .unwrap();
        pipeline
            .inject(inbound("ext", "bob", "carol", Payload::single("Greeting", "text", "y")))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            peak.load(Ordering::SeqCst),
            2,
            "different threads (bob[alice], bob[carol]) must run concurrently"
        );

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn dispatch_serial_and_ordered_within_thread() {
        // Two messages on the SAME delivery thread must never overlap (peak concurrency 1)
        // and must run in arrival order — the bob[alice] state-safety invariant.
        use std::sync::atomic::{AtomicUsize, Ordering};

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let a = active.clone();
        let p = peak.clone();
        let o = order.clone();

        let bob = FnHandler(move |payload: ValidatedPayload, _ctx: HandlerContext| {
            let a = a.clone();
            let p = p.clone();
            let o = o.clone();
            Box::pin(async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                o.lock().await.push(text_of(&payload));
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("bob", "Greeting", bob, false, vec![], "Bob", None);
        let mut pipeline = Pipeline::new(registry, ThreadRegistry::new());
        pipeline.run();

        pipeline
            .inject(inbound("ext", "bob", "alice", Payload::single("Greeting", "text", "first")))
            .await
            .unwrap();
        pipeline
            .inject(inbound("ext", "bob", "alice", Payload::single("Greeting", "text", "second")))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "same thread must be serialized — never two handlers at once for one instance"
        );
        assert_eq!(
            *order.lock().await,
            vec!["first".to_string(), "second".to_string()],
            "same-thread messages must run in arrival order"
        );

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_async_delegation_ack_then_callback() {
        // The full async-delegation loop:
        //   user → bob "book flight"
        //   bob SPAWNS travelagent (async), gets an immediate ack, tells user "I'll let you know"
        //   travelagent works (slowly), replies with the ticket — routed back to bob
        //   bob tells user the result
        // Proves: the caller isn't blocked, the callee is oblivious to who called it (the
        // pipeline routes the result home via the chain), and the obligation is observed.
        use std::sync::Mutex as StdMutex;

        // Observer records opened/closed call edges for the host ledger.
        #[derive(Default)]
        struct Recorder {
            opens: StdMutex<Vec<(String, String, String, CallKind)>>,
            closes: StdMutex<Vec<String>>,
        }
        impl DispatchObserver for Recorder {
            fn on_open(&self, caller: &str, callee: &str, thread: &str, kind: CallKind) {
                self.opens.lock().unwrap().push((
                    caller.to_string(),
                    callee.to_string(),
                    thread.to_string(),
                    kind,
                ));
            }
            fn on_close(&self, thread: &str) {
                self.closes.lock().unwrap().push(thread.to_string());
            }
        }

        // "user" records what Bob sends back to it.
        let user_msgs = Arc::new(StdMutex::new(Vec::<String>::new()));
        let um = user_msgs.clone();
        let user = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            let um = um.clone();
            Box::pin(async move {
                um.lock().unwrap().push(text_of(&p));
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });

        // "bob" — the outer skin. Book → spawn; ack → interim status; result → final answer.
        let bob = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                let r = match p.tag.as_str() {
                    "BookFlight" => HandlerResponse::Spawn {
                        to: "travelagent".into(),
                        payload: Payload::single("FindFlight", "route", "SFO-JFK"),
                    },
                    "SpawnAck" => HandlerResponse::Reply {
                        payload: Payload::single(
                            "Status",
                            "text",
                            "I contacted the travel agent, I'll let you know",
                        ),
                    },
                    "ToolResponse" => {
                        let ticket = p
                            .value
                            .get("result")
                            .and_then(|v| v.as_text())
                            .unwrap_or("")
                            .to_string();
                        HandlerResponse::Reply {
                            payload: Payload::single("Status", "text", format!("found it: {ticket}")),
                        }
                    }
                    _ => HandlerResponse::None,
                };
                Ok(r) as Result<HandlerResponse, PipelineError>
            })
        });

        // "travelagent" — does slow work, replies. Oblivious to who spawned it.
        let travelagent = FnHandler(move |_p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                Ok(HandlerResponse::Reply {
                    payload: Payload::new(
                        "ToolResponse",
                        PayloadValue::record([
                            ("success", PayloadValue::Boolean(true)),
                            ("result", PayloadValue::text("ticket ABC123")),
                        ]),
                    ),
                }) as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        // bob is an agent allowed to message travelagent and reply to user.
        let bob_peers = vec!["travelagent".to_string(), "user".to_string()];
        registry.register("bob", "BookFlight", bob, true, bob_peers.clone(), "Bob", None);
        registry.routing.register("bob", "SpawnAck", true, bob_peers.clone(), "Bob");
        registry.routing.register("bob", "ToolResponse", true, bob_peers, "Bob");
        registry.register("travelagent", "FindFlight", travelagent, false, vec![], "TravelAgent", None);
        registry.register("user", "Status", user, false, vec![], "User", None);

        let observer = Arc::new(Recorder::default());
        let mut pipeline = Pipeline::new(registry, ThreadRegistry::new());
        pipeline.with_dispatch_observer(observer.clone());
        pipeline.run();

        pipeline
            .inject(inbound("user", "bob", "T0", Payload::single("BookFlight", "route", "SFO-JFK")))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // User got the interim status first, then the eventual result — in order.
        let msgs = user_msgs.lock().unwrap().clone();
        assert_eq!(msgs.len(), 2, "user should receive interim status then result, got {msgs:?}");
        assert!(msgs[0].contains("I'll let you know"), "first should be the ack status: {}", msgs[0]);
        assert!(msgs[1].contains("ticket ABC123"), "second should be the result: {}", msgs[1]);

        // Exactly one ASYNC edge opened (bob → travelagent), and it later closed.
        let opens = observer.opens.lock().unwrap().clone();
        assert_eq!(opens.len(), 1, "one async edge should have opened");
        assert_eq!((opens[0].0.as_str(), opens[0].1.as_str(), opens[0].3), ("bob", "travelagent", CallKind::Async));
        let closes = observer.closes.lock().unwrap().clone();
        assert!(closes.contains(&opens[0].2), "the async edge should have closed when travelagent replied");

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn send_emits_sync_open_and_close() {
        // A synchronous Send opens a Sync call edge (the caller is now suspended) and closes
        // it when the callee replies — the visibility the host needs to time out a hung
        // sub-call ("the handler informs the host that a tool is running").
        use std::sync::Mutex as StdMutex;

        #[derive(Default)]
        struct Recorder {
            opens: StdMutex<Vec<(String, String, String, CallKind)>>,
            closes: StdMutex<Vec<String>>,
        }
        impl DispatchObserver for Recorder {
            fn on_open(&self, caller: &str, callee: &str, thread: &str, kind: CallKind) {
                self.opens.lock().unwrap().push((caller.into(), callee.into(), thread.into(), kind));
            }
            fn on_close(&self, thread: &str) {
                self.closes.lock().unwrap().push(thread.into());
            }
        }

        // parent: first call (Req) → Send to child; later (child's reply) → None.
        let parent = FnHandler(move |p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                let r = if p.tag == "Req" {
                    HandlerResponse::Send { to: "child".into(), payload: Payload::single("ChildReq", "x", "go") }
                } else {
                    HandlerResponse::None
                };
                Ok(r) as Result<HandlerResponse, PipelineError>
            })
        });
        let child = FnHandler(|_p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::Reply { payload: Payload::single("ChildResp", "x", "done") })
                    as Result<HandlerResponse, PipelineError>
            })
        });

        let mut registry = ListenerRegistry::new();
        registry.register("parent", "Req", parent, false, vec![], "Parent", None);
        registry.routing.register("parent", "ChildResp", false, vec![], "Parent");
        registry.register("child", "ChildReq", child, false, vec![], "Child", None);

        let observer = Arc::new(Recorder::default());
        let mut pipeline = Pipeline::new(registry, ThreadRegistry::new());
        pipeline.with_dispatch_observer(observer.clone());
        pipeline.run();

        pipeline
            .inject(inbound("sender", "parent", "T0", Payload::single("Req", "x", "start")))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let opens = observer.opens.lock().unwrap().clone();
        assert_eq!(opens.len(), 1, "a Send should open exactly one edge, got {opens:?}");
        assert_eq!((opens[0].0.as_str(), opens[0].1.as_str(), opens[0].3), ("parent", "child", CallKind::Sync));
        let closes = observer.closes.lock().unwrap().clone();
        assert!(closes.contains(&opens[0].2), "the sync edge should close when child replies");

        pipeline.shutdown().await;
    }

    #[tokio::test]
    async fn federation_egress_routes_remote_namespace() {
        use crate::federation::{FederationEgress, Peer, PeerDirectory};
        use crate::wire::Address;
        use std::sync::Arc as StdArc;

        // A local listener "bob" that must NOT receive a remote-addressed message.
        let local_hits = Arc::new(Mutex::new(0u32));
        let lh = local_hits.clone();
        let bob = FnHandler(move |_p: ValidatedPayload, _ctx: HandlerContext| {
            let lh = lh.clone();
            Box::pin(async move {
                *lh.lock().await += 1;
                Ok(HandlerResponse::None) as Result<HandlerResponse, PipelineError>
            })
        });
        let mut registry = ListenerRegistry::new();
        registry.register("bob", "Greeting", bob, false, vec![], "local bob", None);
        let threads = ThreadRegistry::new();
        let mut pipeline = Pipeline::new(registry, threads);

        // "ringhub" is a registered remote peer node.
        let mut dir = PeerDirectory::new();
        dir.register(Peer {
            namespace: "ringhub".into(),
            endpoint: "x".into(),
            key: [0u8; 32],
            inbound_provenance: Provenance::EMPTY,
        });
        let (fed_tx, mut fed_rx) = tokio::sync::mpsc::channel(8);
        pipeline.with_federation(FederationEgress {
            directory: StdArc::new(dir),
            tx: fed_tx,
        });
        pipeline.run();

        // Address to ringhub.bob → leading segment "ringhub" is remote → federation egress.
        let env = Envelope {
            meta: Meta {
                from: "ext".into(),
                to: Some(Address::parse("ringhub.bob").unwrap()),
                thread: "t-fed".into(),
                provenance: Provenance::EMPTY,
            },
            payload: Payload::single("Greeting", "text", "hi"),
        };
        pipeline.inject(encode_envelope(&env).unwrap()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // The federation channel received it; local "bob" did not.
        let got = fed_rx.recv().await.unwrap();
        assert_eq!(got.meta.to.as_ref().unwrap().to_string(), "ringhub.bob");
        assert_eq!(*local_hits.lock().await, 0);

        pipeline.shutdown().await;
    }
}
