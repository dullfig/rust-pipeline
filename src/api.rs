//! The public API of rust-pipeline — the only supported way other projects use it.
//!
//! Everything a caller needs is re-exported here; nothing else in the crate is part of
//! the contract. The internal modules (`codec`, `wire`, `pipeline`, …) are still `pub`
//! for now so existing callers keep compiling, but they are `#[doc(hidden)]` and may
//! change without notice — import from `rust_pipeline::api` (or the prelude, which is
//! this module re-exported).
//!
//! Every item is a re-export or a one-line wrapper: this file moves no code, it only draws
//! the boundary.
//!
//! ```text
//! [Raw Bytes] → decode → validate(schema) → route → enforce(peers) → dispatch(handler)
//!      ↑                                                                  |
//!      └────── encode to raw bytes (UNTRUSTED) ────────────────────────────┘
//! ```

// ── Running a pipeline ───────────────────────────────────────────────

/// The self-feeding pipeline: four tokio stages joined by bounded channels.
///
/// Build it with `let mut p = Pipeline::new(registry, threads)`, install hooks
/// (`add_middleware`, `with_dispatch_observer`, `with_federation`) **before** `p.run()`
/// (needs `&mut`, must be inside a tokio runtime), then feed it with
/// `p.inject(raw_bytes).await` and stop it with `p.shutdown().await` (consumes it).
/// `inject` and `shutdown` are `async`.
///
/// # Arguments
/// * `new(registry, threads)` - the listeners to dispatch to and the thread state to use
///
/// # Returns
/// `inject` returns once the bytes are queued; `ingress_tx` / `thread_failer` hand out
/// clonable handles (call them after `run()`).
///
/// # Errors
/// `inject` returns `PipelineError` when the pipeline is not running (ingress closed).
/// Messages that fail parsing, validation, routing or peer checks are dropped and logged
/// inside the pipeline — they never surface as errors to the injector.
pub use crate::pipeline::Pipeline;

/// The set of listeners (handlers) a pipeline dispatches to.
///
/// `register(name, payload_tag, handler, is_agent, peers, description, schema)` adds one
/// listener; the routing key is `name.payload_tag` (lowercase).
///
/// # Arguments
/// * `register(name, ..)` - listener name, used as the address segment
/// * `register(.., payload_tag, ..)` - the payload tag this listener accepts
/// * `register(.., handler, ..)` - anything implementing [`Handler`]
/// * `register(.., is_agent, ..)` - agents are peer-restricted: they may only message `peers`
/// * `register(.., peers, ..)` - names an agent may send to (empty = no one; deny by default)
/// * `register(.., description, ..)` - human-readable purpose
/// * `register(.., schema)` - optional [`PayloadSchema`] enforced before dispatch
///
/// # Errors
/// Registration cannot fail; registering the same name again replaces the handler.
pub use crate::registry::ListenerRegistry;

/// Thread registry: maps opaque thread UUIDs to dot-separated call chains
/// (e.g. `console.router.greeter`) so UUIDs hide topology.
///
/// Callers use `initialize_root`, `get_or_create`, `start_chain`, `extend_chain`,
/// `prune_for_response`, `lookup` and `cleanup` to manage conversation threads.
///
/// # Returns
/// Methods that create threads return the new UUID; lookups return `Option`.
///
/// # Errors
/// None of its methods fail; an unknown thread yields `None` (or an empty provenance).
pub use crate::thread::ThreadRegistry;

/// Every way the pipeline can fail: parse, validation, routing, peer violation, thread,
/// handler and serialization errors.
///
/// Handlers return it (via [`HandlerResult`]) to report failure; callers match on it
/// to tell, e.g., [`PipelineError::PeerViolation`] from [`PipelineError::NoRoute`].
pub use crate::error::PipelineError;

/// Shorthand for `Result<T, PipelineError>`, returned by every fallible pipeline call.
pub use crate::error::PipelineResult;

// ── Writing handlers ─────────────────────────────────────────────────

/// The trait every listener implements.
///
/// `handle(payload, ctx)` receives an already validated, typed payload and returns what
/// to do next. The response is never trusted: the pipeline re-encodes it to bytes and
/// runs it through the full gauntlet again.
///
/// # Arguments
/// * `handle(payload, ..)` - the validated payload ([`ValidatedPayload`])
/// * `handle(.., ctx)` - pipeline-verified metadata ([`HandlerContext`])
///
/// # Returns
/// A [`HandlerResult`] carrying the [`HandlerResponse`].
///
/// # Errors
/// Return `Err(PipelineError::Handler(..))` to report failure; panics are caught and
/// surfaced as `PipelineError::HandlerPanic`.
pub use crate::handler::Handler;

/// Adapter that turns an async closure `Fn(ValidatedPayload, HandlerContext) -> Pin<Box<Future<Output = HandlerResult>>>`
/// into a [`Handler`].
///
/// # Arguments
/// * `.0` - the closure
///
/// # Errors
/// Whatever the closure returns.
pub use crate::handler::FnHandler;

/// Verified metadata handed to a handler: thread id, sender address, its own address and
/// the message's provenance set.
///
/// Set by the pipeline; a handler cannot forge it. Provenance is observation-only — a
/// handler must not gate on it.
pub use crate::handler::HandlerContext;

/// What a handler wants to happen next.
///
/// * `Reply` - answer back up the thread chain
/// * `Send` - synchronous sub-call: suspend until the callee replies
/// * `Spawn` - asynchronous delegation: continue now, callback later
/// * `None` - nothing; a waiting parent gets a synthesized ACK
///
/// Helpers: `HandlerResponse::reply(payload)`, `::send(to, payload)`, `::spawn(to, payload)`.
///
/// # Errors
/// A `Send`/`Spawn` to a non-peer is rejected by the pipeline with
/// `PipelineError::PeerViolation`; it is not an error at construction time.
pub use crate::handler::HandlerResponse;

/// The return type of [`Handler::handle`]: `Result<HandlerResponse, PipelineError>`.
pub use crate::handler::HandlerResult;

/// The payload a handler receives, after decode and schema validation.
///
/// Holds the `tag` and a typed [`PayloadValue`] — never raw bytes.
/// `to_payload()` clones it into an outgoing [`Payload`].
pub use crate::handler::ValidatedPayload;

// ── Middleware ───────────────────────────────────────────────────────

/// Intercepts messages around dispatch (gates, guards, transforms).
///
/// `pre_dispatch` runs in registration order and may continue, replace the payload, or
/// short-circuit with its own response; `post_dispatch` runs in reverse order and may
/// replace the handler's response. Both default to pass-through.
///
/// # Arguments
/// * `pre_dispatch(meta, ..)` / `post_dispatch(meta, ..)` - [`DispatchMeta`] for the message
/// * `pre_dispatch(.., payload)` / `post_dispatch(.., result)` - the payload, or the handler's result
///
/// # Returns
/// A [`PreDispatchVerdict`] or [`PostDispatchVerdict`].
///
/// # Errors
/// Middleware does not fail; to block a message, short-circuit with a response.
pub use crate::middleware::Middleware;

/// Verified facts about the message being dispatched (from, to, thread id, payload tag),
/// as seen by middleware.
pub use crate::middleware::DispatchMeta;

/// A middleware's decision before dispatch: continue, continue with a replaced payload,
/// or short-circuit with a response.
pub use crate::middleware::PreDispatchVerdict;

/// A middleware's decision after dispatch: pass the response through or replace it.
pub use crate::middleware::PostDispatchVerdict;

// ── Supervising call edges ───────────────────────────────────────────

/// Host hook told when a call edge opens and closes, so the host can set deadlines for
/// hung `Send`/`Spawn` calls and keep an in-flight ledger.
///
/// Install with `Pipeline::with_dispatch_observer`. `on_open`/`on_close` run inline in
/// dispatch and must be cheap.
///
/// # Arguments
/// * `on_open(caller, callee, ..)` - the two ends of the edge
/// * `on_open(.., thread, ..)` / `on_close(thread)` - the thread the response is expected on
/// * `on_open(.., kind)` - [`CallKind`]
///
/// # Errors
/// Callbacks return nothing and cannot fail the pipeline.
pub use crate::pipeline::DispatchObserver;

/// Whether an in-flight call edge is synchronous or asynchronous.
///
/// Variants: `CallKind::Sync` — opened by a `HandlerResponse::Send` (caller suspended until
/// the reply); `CallKind::Async` — opened by a `HandlerResponse::Spawn` (caller continues,
/// receives a callback later).
pub use crate::pipeline::CallKind;

/// The recovery half of [`DispatchObserver`]: injects a `Timeout` back to the caller
/// waiting on a hung edge, and makes any late real reply drop.
///
/// Get one from `Pipeline::thread_failer()` after `run()`.
///
/// # Arguments
/// * `fail(thread_id, reason)` - the hung edge's thread and a human-readable reason
///   (`async`: `failer.fail(thread_id, reason).await`)
///
/// # Returns
/// `Ok(true)` if a waiting caller was failed, `Ok(false)` if the thread had none
/// (already resolved, unknown or stale).
///
/// # Errors
/// `PipelineError` when the `Timeout` cannot be encoded or the ingress is closed. The
/// caller must have a route for the `Timeout` tag, or the injected failure is dropped.
pub use crate::pipeline::ThreadFailer;

// ── Wire types ───────────────────────────────────────────────────────

/// The one canonical message: [`Meta`] header plus [`Payload`].
pub use crate::wire::Envelope;

/// Envelope header: `from` (last hop; audit only), optional `to`, the opaque `thread`
/// id, and the durable [`Provenance`] set.
pub use crate::wire::Meta;

/// A typed payload: a `tag` (selects the operation/schema) and a [`PayloadValue`].
///
/// `Payload::single(tag, field, text)` builds the common one-text-field case.
pub use crate::wire::Payload;

/// A typed payload value: record, sequence, text, integers, real, boolean, blob or nil.
///
/// Helpers: `text`, `record`, `get(field)`, `as_text()`.
pub use crate::wire::PayloadValue;

/// One named field of a record [`PayloadValue`].
pub use crate::wire::Field;

/// A hierarchical address, e.g. `agentos.concierge[alice]`.
///
/// `Address::flat(name)` builds a one-segment address; `Address::parse(s)` parses one.
/// Accessors expose the target, organism, namespace, node, instance key, buffer and
/// cache keys.
///
/// # Errors
/// `Address::parse` returns [`AddressError`] for empty input, empty segments,
/// unbalanced brackets or empty names.
pub use crate::wire::Address;

/// One segment of an [`Address`]: a name plus optional instance key.
pub use crate::wire::Segment;

/// Why an address string failed to parse.
pub use crate::wire::AddressError;

/// The durable origin set (a 128-bit bit set) carried on every envelope and unioned
/// downstream.
///
/// rust-pipeline carries it; the host decides what each bit means and enforces policy.
/// `Provenance::EMPTY` means *not cleared*, not *clean*.
pub use crate::wire::Provenance;

/// Encodes a typed envelope to wire bytes — the only way to produce bytes to inject.
///
/// # Arguments
/// * `env` - the envelope to encode
///
/// # Returns
/// The wire bytes (XML today, transitional).
///
/// # Errors
/// `PipelineError::Serialization` when the envelope cannot be written.
pub fn encode_envelope(env: &Envelope) -> PipelineResult<Vec<u8>> {
    crate::codec::encode_envelope(env)
}

/// Decodes wire bytes into a typed envelope.
///
/// Missing provenance decodes as the empty set; missing `to` as `None`.
///
/// # Arguments
/// * `raw` - untrusted wire bytes
///
/// # Returns
/// The decoded [`Envelope`].
///
/// # Errors
/// `PipelineError::EnvelopeParse`, `MissingMeta` or `Xml` when the bytes are not a valid
/// envelope.
pub fn decode_envelope(raw: &[u8]) -> PipelineResult<Envelope> {
    crate::codec::decode_envelope(raw)
}

// ── Schemas ──────────────────────────────────────────────────────────

/// The expected shape of a payload: root tag, fields, and whether unknown fields are
/// rejected (`strict`).
pub use crate::validation::PayloadSchema;

/// One field of a [`PayloadSchema`]: whether it is required and its [`FieldType`].
pub use crate::validation::FieldSchema;

/// A field's type: string, integer, boolean, nested schema, or any.
pub use crate::validation::FieldType;

/// Schemas keyed by payload tag; `validate(tag, bytes)` / `validate_value(tag, value)`
/// check a payload against the registered schema.
///
/// # Errors
/// `PipelineError::Validation` when the payload does not match.
pub use crate::validation::SchemaRegistry;

/// Checks a payload's bytes against a schema, before or outside the pipeline.
///
/// # Arguments
/// * `payload_xml` - the payload element's bytes
/// * `schema` - the schema to check against
///
/// # Returns
/// `Ok(())` when the payload matches.
///
/// # Errors
/// `PipelineError::Validation` describing the first mismatch (wrong root, missing
/// required field, unexpected field under `strict`), or `PipelineError::Xml` for
/// malformed bytes.
pub fn validate_payload(payload_xml: &[u8], schema: &PayloadSchema) -> PipelineResult<()> {
    crate::validation::validate_payload(payload_xml, schema)
}

// ── Instance routing ─────────────────────────────────────────────────

/// Materialize-then-inject front over a pipeline's ingress.
///
/// `Switchboard::new(materializer, pipeline.ingress_tx())`, then `route(envelope)`
/// resolves the destination instance, stamps its thread and injects. `route` is `async`:
/// `switchboard.route(envelope).await`.
///
/// # Arguments
/// * `route(envelope)` - an envelope whose `meta.to` names the destination
///
/// # Errors
/// [`SwitchboardError`] when there is no destination, materialization fails, the ingress
/// is closed, or encoding fails. Nothing is injected on failure (fail-closed).
pub use crate::switchboard::Switchboard;

/// Host hook that resolves an instance address to its delivery thread, creating the
/// instance on first use; `evict` releases it.
///
/// # Arguments
/// * `resolve(address)` - the instance (and optional buffer) address
///
/// # Returns
/// The delivery thread id.
///
/// # Errors
/// Return `Err(String)` to refuse; the switchboard reports it as
/// `SwitchboardError::Materialize`.
pub use crate::switchboard::Materializer;

/// Why [`Switchboard::route`] failed.
///
/// Variants: `NoDestination` (no `meta.to`), `Materialize(String)` (the
/// [`Materializer`] refused), `IngressClosed`, `Encode(PipelineError)`.
pub use crate::switchboard::SwitchboardError;

// ── Federation ───────────────────────────────────────────────────────

/// The node-boundary server: seals and sends envelopes to remote nodes, and opens,
/// stamps and locally delivers inbound frames.
///
/// `FederationServer::new(node, directory, transport, local, authorizer)`; then
/// `send(envelope).await`, `receive(frame).await`, and `run_egress(rx).await` to drain the
/// pipeline's federation-egress channel (all three are `async`).
///
/// # Arguments
/// * `new(node, ..)` - this node's namespace
/// * `new(.., directory, ..)` - the known peers
/// * `new(.., transport, ..)` - moves sealed frames ([`Transport`])
/// * `new(.., local, ..)` - where opened envelopes go ([`LocalDelivery`])
/// * `new(.., authorizer)` - may an authenticated origin reach its target ([`Authorizer`])
/// * `send(envelope)` - an envelope addressed to a registered remote node
/// * `receive(frame)` - a sealed frame from the transport
///
/// # Errors
/// `send` and `receive` return [`FederationError`] (unknown peer, bad frame, failed
/// authentication, unauthorized, transport or delivery failure).
pub use crate::federation::FederationServer;

/// The egress hook installed with `Pipeline::with_federation`: messages addressed to a
/// registered remote node go here instead of to a local listener.
pub use crate::federation::FederationEgress;

/// Moves sealed frames to a peer endpoint (TCP, websocket, …) — supplied by the host.
///
/// # Errors
/// Return `Err(String)` on send failure; surfaced as `FederationError::Transport`.
pub use crate::federation::Transport;

/// Where opened inbound envelopes are delivered locally (normally the switchboard).
///
/// # Errors
/// Return `Err(String)` on failure; surfaced as `FederationError::Delivery`.
pub use crate::federation::LocalDelivery;

/// A registered remote node: namespace, endpoint, shared key, and the provenance stamped
/// on everything received from it.
pub use crate::federation::Peer;

/// Known peers by namespace; `is_remote(ns)` decides local vs. federated routing.
pub use crate::federation::PeerDirectory;

/// A 32-byte pre-shared symmetric key for one peer link.
pub use crate::federation::PeerKey;

/// Host policy at the federation seam: `authorize(from, to)` decides whether the
/// edge-stamped (unforgeable) origin `from` may reach target `to`. Called after stamping,
/// before local delivery; rejection fails closed.
///
/// # Arguments
/// * `authorize(from, ..)` - the authenticated origin address
/// * `authorize(.., to)` - the target address
///
/// # Errors
/// Return `Err(reason)` to refuse; surfaced as `FederationError::Unauthorized`.
pub use crate::federation::Authorizer;

/// An [`Authorizer`] that admits every authenticated peer (tests, trusted setups).
pub use crate::federation::AllowAll;

/// Everything that can go wrong at the federation boundary.
///
/// Variants: `UnknownPeer(String)`, `MalformedFrame`, `UnsupportedVersion(u8)`,
/// `UnsupportedAuth(u8)`, `OpenFailed`, `Nonce`, `NoDestination`, `NotRemote(String)`,
/// `Transport(String)`, `Delivery(String)`, `Unauthorized(String)`, `Codec(PipelineError)`.
pub use crate::federation::FederationError;

/// Seals an envelope for a peer: encodes it and encrypts it with the peer's key.
///
/// # Arguments
/// * `envelope` - the envelope to send
/// * `sender` - this node's namespace (authenticated by the key)
/// * `key` - the peer link's shared key
///
/// # Returns
/// The sealed frame bytes.
///
/// # Errors
/// `FederationError::Codec` when encoding fails, `FederationError::Nonce` when no nonce
/// can be generated, `FederationError::OpenFailed` when encryption fails.
pub fn seal(envelope: &Envelope, sender: &str, key: &PeerKey) -> Result<Vec<u8>, FederationError> {
    crate::federation::seal(envelope, sender, key)
}

/// Opens a sealed frame, authenticating the sender against the peer directory.
///
/// # Arguments
/// * `frame` - the received bytes
/// * `directory` - the known peers and their keys
///
/// # Returns
/// The envelope and the authenticated sender namespace. The envelope is still untrusted.
///
/// # Errors
/// `FederationError::UnknownPeer`, `MalformedFrame`, `UnsupportedVersion`,
/// `UnsupportedAuth`, `OpenFailed` (wrong key or tampering) or `Codec`.
pub fn open(frame: &[u8], directory: &PeerDirectory) -> Result<(Envelope, String), FederationError> {
    crate::federation::open(frame, directory)
}
