# rust-pipeline API (v1.0.0)

## Overview
rust-pipeline is a message pipeline. Raw bytes are decoded, validated against a schema, routed to a listener, checked against peer rules, and dispatched to a handler. A handler's response is re-encoded to bytes and goes through the same checks again, because responses are never trusted. This module is the only supported way for other projects to use the crate. Callers import from `rust_pipeline::api` or from the prelude, which re-exports this module.

## Getting started
```rust
use rust_pipeline::api::*;
```

```rust
use rust_pipeline::api::*;

let registry: ListenerRegistry = /* register your listeners */;
let pipeline = Pipeline::new(registry, /* thread state */);
pipeline.run();
let raw = encode_envelope(&envelope)?;   // envelope: an Envelope you built
pipeline.inject(raw)?;
pipeline.shutdown();
```

## Reference

### `Pipeline`
**Signature:** `pub use crate::pipeline::Pipeline;`
**Purpose:** The self-feeding pipeline: four tokio stages joined by bounded channels. Build it with `Pipeline::new(registry, threads)`, install hooks (`add_middleware`, `with_dispatch_observer`, `with_federation`) before `run()`, feed it with `inject(raw_bytes)`, and stop it with `shutdown()`.
**Parameters:** `new(registry, threads)`: the listeners to dispatch to, and the thread state to use.
**Returns:** `inject` returns once the bytes are queued. `ingress_tx` and `thread_failer` hand out clonable handles; call them after `run()`.
**Errors:** `inject` returns `PipelineError` when the pipeline is not running (ingress closed). Messages that fail parsing, validation, routing or peer checks are dropped and logged inside the pipeline. They never surface as errors to the injector.
**Example:**
```rust
let pipeline = Pipeline::new(registry, /* thread state */);
pipeline.run();
pipeline.inject(raw_bytes)?;
pipeline.shutdown();
```

### `ListenerRegistry`
**Signature:** `pub use crate::registry::ListenerRegistry;`
**Purpose:** The set of listeners (handlers) a pipeline dispatches to. `register(name, payload_tag, handler, is_agent, peers, description, schema)` adds one listener. The routing key is `name.payload_tag` in lowercase.
**Parameters:** `register(name, ..)`: the listener name, used as the address segment. `register(.., payload_tag, ..)`: the payload tag this listener accepts. `register(.., handler, ..)`: anything implementing `Handler`. `register(.., is_agent, ..)`: agents are peer-restricted and may only message `peers`. `register(.., peers, ..)`: the names an agent may send to; empty means no one (deny by default). `register(.., description, ..)`: a human-readable purpose. `register(.., schema)`: an optional `PayloadSchema`, enforced before dispatch.
**Returns:** Not stated.
**Errors:** Registration cannot fail. Registering the same name again replaces the handler.
**Example:**
```rust
registry.register("greeter", "hello", handler, false, vec![], "Greets people", None);
```

### `ThreadRegistry`
**Signature:** `pub use crate::thread::ThreadRegistry;`
**Purpose:** Maps opaque thread UUIDs to dot-separated call chains (for example `console.router.greeter`), so UUIDs hide topology. Callers use `initialize_root`, `get_or_create`, `start_chain`, `extend_chain`, `prune_for_response`, `lookup` and `cleanup` to manage conversation threads.
**Parameters:** None documented on the type; see the individual methods.
**Returns:** Methods that create threads return the new UUID. Lookups return `Option`.
**Errors:** None of its methods fail. An unknown thread yields `None` (or an empty provenance).
**Example:**
```rust
let found = threads.lookup(thread_id); // None when the thread is unknown
```

### `PipelineError`
**Signature:** `pub use crate::error::PipelineError;`
**Purpose:** Every way the pipeline can fail: parse, validation, routing, peer violation, thread, handler and serialization errors. Handlers return it (via `HandlerResult`) to report failure. Callers match on it to tell, for example, `PeerViolation` from `NoRoute`.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** Not applicable; this is the error type itself.
**Example:**
```rust
match pipeline.inject(raw) {
    Err(PipelineError::PeerViolation { .. }) => { /* a peer rule rejected the message */ }
    _ => {}
}
```

### `PipelineResult`
**Signature:** `pub use crate::error::PipelineResult;`
**Purpose:** Shorthand for `Result<T, PipelineError>`, returned by every fallible pipeline call.
**Parameters:** None.
**Returns:** Nothing; it is a type alias.
**Errors:** None of its own; it carries `PipelineError`.
**Example:**
```rust
fn send(p: &Pipeline, raw: Vec<u8>) -> PipelineResult<()> {
    p.inject(raw)
}
```

### `Handler`
**Signature:** `pub use crate::handler::Handler;`
**Purpose:** The trait every listener implements. `handle(payload, ctx)` receives an already validated, typed payload and returns what to do next. The response is never trusted: the pipeline re-encodes it to bytes and runs it through the full gauntlet again.
**Parameters:** `handle(payload, ..)`: the validated payload (`ValidatedPayload`). `handle(.., ctx)`: pipeline-verified metadata (`HandlerContext`).
**Returns:** A `HandlerResult` carrying the `HandlerResponse`.
**Errors:** Return `Err(PipelineError::Handler(..))` to report failure. Panics are caught and surfaced as `PipelineError::HandlerPanic`.
**Example:**
```rust
// Inside an `impl Handler`: echo the payload back up the thread chain.
Ok(HandlerResponse::reply(payload.to_payload()))
```

### `FnHandler`
**Signature:** `pub use crate::handler::FnHandler;`
**Purpose:** Adapter that turns an async closure `Fn(ValidatedPayload, HandlerContext) -> Pin<Box<Future<Output = HandlerResult>>>` into a `Handler`.
**Parameters:** `.0`: the closure.
**Returns:** A `Handler`.
**Errors:** Whatever the closure returns.
**Example:**
```rust
let handler = FnHandler(|payload, _ctx| Box::pin(async move {
    Ok(HandlerResponse::reply(payload.to_payload()))
}));
```

### `HandlerContext`
**Signature:** `pub use crate::handler::HandlerContext;`
**Purpose:** Verified metadata handed to a handler: the thread id, the sender address, its own address, and the message's provenance set. The pipeline sets it, and a handler cannot forge it. Provenance is observation-only; a handler must not gate on it.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// Passed in by the pipeline. Read it, but do not branch on its provenance.
fn handle(&self, payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult { /* ... */ }
```

### `HandlerResponse`
**Signature:** `pub use crate::handler::HandlerResponse;`
**Purpose:** What a handler wants to happen next.
- `Reply`: answer back up the thread chain.
- `Send`: a synchronous sub-call. Suspend until the callee replies.
- `Spawn`: asynchronous delegation. Continue now and receive a callback later.
- `None`: nothing. A waiting parent gets a synthesized ACK.

Helpers: `HandlerResponse::reply(payload)`, `::send(to, payload)`, `::spawn(to, payload)`.
**Parameters:** `reply(payload)`: the payload to answer with. `send(to, payload)` and `spawn(to, payload)`: the callee and the payload.
**Returns:** The response variant chosen by the helper.
**Errors:** A `Send` or `Spawn` to a non-peer is rejected by the pipeline with `PipelineError::PeerViolation`. It is not an error at construction time.
**Example:**
```rust
Ok(HandlerResponse::send(callee, payload)) // rejected with PeerViolation unless callee is a peer
```

### `HandlerResult`
**Signature:** `pub use crate::handler::HandlerResult;`
**Purpose:** The return type of `Handler::handle`: `Result<HandlerResponse, PipelineError>`.
**Parameters:** None.
**Returns:** Nothing; it is a type alias.
**Errors:** None of its own; see `PipelineError`.
**Example:**
```rust
fn finish(resp: HandlerResponse) -> HandlerResult {
    Ok(resp)
}
```

### `ValidatedPayload`
**Signature:** `pub use crate::handler::ValidatedPayload;`
**Purpose:** The payload a handler receives, after decode and schema validation. It holds the `tag` and a typed `PayloadValue`, never raw bytes. `to_payload()` clones it into an outgoing `Payload`.
**Parameters:** None.
**Returns:** `to_payload()` returns an outgoing `Payload`.
**Errors:** None.
**Example:**
```rust
let outgoing: Payload = payload.to_payload();
```

### `Middleware`
**Signature:** `pub use crate::middleware::Middleware;`
**Purpose:** Intercepts messages around dispatch (gates, guards, transforms). `pre_dispatch` runs in registration order and may continue, replace the payload, or short-circuit with its own response. `post_dispatch` runs in reverse order and may replace the handler's response. Both default to pass-through.
**Parameters:** `pre_dispatch(meta, .., payload)` and `post_dispatch(meta, .., result)`: the `DispatchMeta` for the message, and the payload or the handler's result.
**Returns:** A `PreDispatchVerdict` or a `PostDispatchVerdict`.
**Errors:** Middleware does not fail. To block a message, short-circuit it with a response.
**Example:**
```rust
impl Middleware for Gate {
    // Override pre_dispatch and/or post_dispatch. Both default to pass-through.
}
pipeline.add_middleware(Gate); // before run()
```

### `DispatchMeta`
**Signature:** `pub use crate::middleware::DispatchMeta;`
**Purpose:** Verified facts about the message being dispatched (from, to, thread id, payload tag), as seen by middleware.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// Given to middleware as `meta`: the verified from, to, thread id and payload tag.
```

### `PreDispatchVerdict`
**Signature:** `pub use crate::middleware::PreDispatchVerdict;`
**Purpose:** A middleware's decision before dispatch: continue, continue with a replaced payload, or short-circuit with a response.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// Returned from pre_dispatch: continue, continue with a replaced payload, or short-circuit.
```

### `PostDispatchVerdict`
**Signature:** `pub use crate::middleware::PostDispatchVerdict;`
**Purpose:** A middleware's decision after dispatch: pass the response through or replace it.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// Returned from post_dispatch: pass the response through, or replace it.
```

### `DispatchObserver`
**Signature:** `pub use crate::pipeline::DispatchObserver;`
**Purpose:** A host hook told when a call edge opens and closes, so the host can set deadlines for hung `Send` and `Spawn` calls and keep an in-flight ledger. Install it with `Pipeline::with_dispatch_observer`. `on_open` and `on_close` run inline in dispatch and must be cheap.
**Parameters:** `on_open(caller, callee, ..)`: the two ends of the edge. `on_open(.., thread, ..)`: the thread the response is expected on. `on_open(.., kind)`: the `CallKind`. `on_close(thread)`: the thread.
**Returns:** Nothing. The callbacks return nothing.
**Errors:** The callbacks cannot fail the pipeline.
**Example:**
```rust
impl DispatchObserver for Ledger {
    // on_open / on_close: keep them cheap.
}
pipeline.with_dispatch_observer(Ledger); // before run()
```

### `CallKind`
**Signature:** `pub use crate::pipeline::CallKind;`
**Purpose:** Whether an in-flight call edge is a synchronous `Send` (the caller is suspended) or an asynchronous `Spawn` (the caller continues and awaits a callback).
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
match kind {
    CallKind::Send => { /* caller is suspended */ }
    CallKind::Spawn => { /* caller continues */ }
}
```

### `ThreadFailer`
**Signature:** `pub use crate::pipeline::ThreadFailer;`
**Purpose:** The recovery half of `DispatchObserver`. It injects a `Timeout` back to the caller waiting on a hung edge, and any late real reply is dropped. Get one from `Pipeline::thread_failer()` after `run()`.
**Parameters:** `fail(thread_id, reason)`: the hung edge's thread, and a human-readable reason.
**Returns:** `Ok(true)` if a waiting caller was failed. `Ok(false)` if the thread had none (already resolved, unknown or stale).
**Errors:** Returns `PipelineError` when the `Timeout` cannot be encoded or the ingress is closed. The caller must have a route for the `Timeout` tag, or the injected failure is dropped.
**Example:**
```rust
let failer = pipeline.thread_failer(); // after run()
let failed = failer.fail(thread_id, "no reply within deadline")?;
```

### `Envelope`
**Signature:** `pub use crate::wire::Envelope;`
**Purpose:** The one canonical message: a `Meta` header plus a `Payload`.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let envelope: Envelope = /* a Meta header plus a Payload */;
let raw = encode_envelope(&envelope)?;
```

### `Meta`
**Signature:** `pub use crate::wire::Meta;`
**Purpose:** The envelope header: `from` (the last hop; for audit only), an optional `to`, the opaque `thread` id, and the durable `Provenance` set.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// The header of an Envelope: from (audit only), to (optional), thread, and provenance.
```

### `Payload`
**Signature:** `pub use crate::wire::Payload;`
**Purpose:** A typed payload: a `tag` (which selects the operation or schema) and a `PayloadValue`. `Payload::single(tag, field, text)` builds the common case of one text field.
**Parameters:** `single(tag, field, text)`: the tag, the field name, and its text value.
**Returns:** A `Payload`.
**Errors:** None documented.
**Example:**
```rust
let greet = Payload::single("greet", "name", "Ada");
```

### `PayloadValue`
**Signature:** `pub use crate::wire::PayloadValue;`
**Purpose:** A typed payload value: record, sequence, text, integers, real, boolean, blob or nil. Helpers: `text`, `record`, `get(field)`, `as_text()`.
**Parameters:** None documented on the type; see the helpers.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let value = PayloadValue::text("Ada");
let name = value.as_text();
```

### `Field`
**Signature:** `pub use crate::wire::Field;`
**Purpose:** One named field of a record `PayloadValue`.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// A named entry inside a record PayloadValue, reached with get(field).
```

### `Address`
**Signature:** `pub use crate::wire::Address;`
**Purpose:** A hierarchical address, for example `agentos.concierge[alice]`. `Address::flat(name)` builds a one-segment address. `Address::parse(s)` parses one. Accessors expose the target, organism, namespace, node, instance key, buffer and cache keys.
**Parameters:** `flat(name)`: the one-segment name. `parse(s)`: the address string.
**Returns:** `flat` returns an `Address`. `parse` returns the parsed address.
**Errors:** `Address::parse` returns `AddressError` for empty input, empty segments, unbalanced brackets or empty names.
**Example:**
```rust
let flat = Address::flat("concierge");
let parsed = Address::parse("agentos.concierge[alice]")?;
```

### `Segment`
**Signature:** `pub use crate::wire::Segment;`
**Purpose:** One segment of an `Address`: a name plus an optional instance key.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// In agentos.concierge[alice], "agentos" and "concierge[alice]" are two Segments.
```

### `AddressError`
**Signature:** `pub use crate::wire::AddressError;`
**Purpose:** Why an address string failed to parse.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** Not applicable; this is the error type itself (see `Address::parse`).
**Example:**
```rust
if let Err(e) = Address::parse("") {
    // e says why the address was rejected
}
```

### `Provenance`
**Signature:** `pub use crate::wire::Provenance;`
**Purpose:** The durable origin set, a 128-bit bit set, carried on every envelope and unioned downstream. rust-pipeline carries it; the host decides what each bit means and enforces policy. `Provenance::EMPTY` means *not cleared*, not *clean*.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let uncleared = Provenance::EMPTY; // "not cleared", not "clean"
```

### `encode_envelope`
**Signature:** `pub fn encode_envelope(env: &Envelope) -> PipelineResult<Vec<u8>>`
**Purpose:** Encodes a typed envelope to wire bytes. This is the only way to produce bytes to inject.
**Parameters:** `env`: the envelope to encode.
**Returns:** The wire bytes (XML today, transitional).
**Errors:** `PipelineError::Serialization` when the envelope cannot be written.
**Example:**
```rust
let raw = encode_envelope(&envelope)?;
pipeline.inject(raw)?;
```

### `decode_envelope`
**Signature:** `pub fn decode_envelope(raw: &[u8]) -> PipelineResult<Envelope>`
**Purpose:** Decodes wire bytes into a typed envelope. Missing provenance decodes as the empty set, and a missing `to` decodes as `None`.
**Parameters:** `raw`: untrusted wire bytes.
**Returns:** The decoded `Envelope`.
**Errors:** `PipelineError::EnvelopeParse`, `MissingMeta` or `Xml` when the bytes are not a valid envelope.
**Example:**
```rust
let env = decode_envelope(&raw)?;
```

### `PayloadSchema`
**Signature:** `pub use crate::validation::PayloadSchema;`
**Purpose:** The expected shape of a payload: its root tag, its fields, and whether unknown fields are rejected (`strict`).
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let schema: PayloadSchema = /* root tag, fields, and strict */;
validate_payload(payload_xml, &schema)?;
```

### `FieldSchema`
**Signature:** `pub use crate::validation::FieldSchema;`
**Purpose:** One field of a `PayloadSchema`: whether it is required, and its `FieldType`.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// One entry in a PayloadSchema's fields: required or optional, plus a FieldType.
```

### `FieldType`
**Signature:** `pub use crate::validation::FieldType;`
**Purpose:** A field's type: string, integer, boolean, nested schema, or any.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// Used in a FieldSchema: string, integer, boolean, nested schema, or any.
```

### `SchemaRegistry`
**Signature:** `pub use crate::validation::SchemaRegistry;`
**Purpose:** Schemas keyed by payload tag. `validate(tag, bytes)` and `validate_value(tag, value)` check a payload against the registered schema.
**Parameters:** `validate(tag, bytes)`: the payload tag and its bytes. `validate_value(tag, value)`: the payload tag and its value.
**Returns:** Not stated.
**Errors:** `PipelineError::Validation` when the payload does not match.
**Example:**
```rust
schemas.validate("greet", &bytes)?;
```

### `validate_payload`
**Signature:** `pub fn validate_payload(payload_xml: &[u8], schema: &PayloadSchema) -> PipelineResult<()>`
**Purpose:** Checks a payload's bytes against a schema, before or outside the pipeline.
**Parameters:** `payload_xml`: the payload element's bytes. `schema`: the schema to check against.
**Returns:** `Ok(())` when the payload matches.
**Errors:** `PipelineError::Validation` describes the first mismatch: wrong root, missing required field, or unexpected field under `strict`. `PipelineError::Xml` is returned for malformed bytes.
**Example:**
```rust
validate_payload(payload_xml, &schema)?;
```

### `Switchboard`
**Signature:** `pub use crate::switchboard::Switchboard;`
**Purpose:** A materialize-then-inject front over a pipeline's ingress. `Switchboard::new(materializer, pipeline.ingress_tx())` builds it. `route(envelope)` then resolves the destination instance, stamps its thread, and injects.
**Parameters:** `new(materializer, ingress)`: the `Materializer` and the pipeline's ingress handle. `route(envelope)`: an envelope whose `meta.to` names the destination.
**Returns:** Not stated.
**Errors:** `SwitchboardError` when there is no destination, materialization fails, the ingress is closed, or encoding fails. Nothing is injected on failure (fail-closed).
**Example:**
```rust
let switchboard = Switchboard::new(materializer, pipeline.ingress_tx());
switchboard.route(envelope)?;
```

### `Materializer`
**Signature:** `pub use crate::switchboard::Materializer;`
**Purpose:** A host hook that resolves an instance address to its delivery thread, creating the instance on first use. `evict` releases it.
**Parameters:** `resolve(address)`: the instance address, and the optional buffer address.
**Returns:** The delivery thread id.
**Errors:** Return `Err(String)` to refuse. The switchboard reports it as `SwitchboardError::Materialize`.
**Example:**
```rust
impl Materializer for Host {
    // resolve: create the instance on first use and return its thread id, or Err(String) to refuse.
}
```

### `SwitchboardError`
**Signature:** `pub use crate::switchboard::SwitchboardError;`
**Purpose:** Why `Switchboard::route` failed.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** Not applicable; this is the error type itself.
**Example:**
```rust
if let Err(SwitchboardError::Materialize { .. }) = switchboard.route(envelope) {
    // the Materializer refused the instance
}
```

### `FederationServer`
**Signature:** `pub use crate::federation::FederationServer;`
**Purpose:** The node-boundary server. It seals and sends envelopes to remote nodes, and opens, stamps and locally delivers inbound frames. Build it with `FederationServer::new(node, directory, transport, local, authorizer)`. Then use `send(envelope)` and `receive(frame)`, and `run_egress(rx)` to drain the pipeline's federation-egress channel.
**Parameters:** `new(node, ..)`: this node's namespace. `new(.., directory, ..)`: the known peers. `new(.., transport, ..)`: the `Transport` that moves sealed frames. `new(.., local, ..)`: the `LocalDelivery` for opened envelopes. `new(.., authorizer)`: the `Authorizer`. `send(envelope)`: an envelope addressed to a registered remote node. `receive(frame)`: a sealed frame from the transport. `run_egress(rx)`: the federation-egress channel.
**Returns:** Not stated.
**Errors:** `send` and `receive` return `FederationError`, covering an unknown peer, a bad frame, failed authentication, unauthorized access, and transport or delivery failure.
**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, AllowAll);
server.receive(frame)?;
```

### `FederationEgress`
**Signature:** `pub use crate::federation::FederationEgress;`
**Purpose:** The egress hook installed with `Pipeline::with_federation`. Messages addressed to a registered remote node go here instead of to a local listener.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
pipeline.with_federation(egress); // before run()
```

### `Transport`
**Signature:** `pub use crate::federation::Transport;`
**Purpose:** Moves sealed frames to a peer endpoint (TCP, websocket, and so on). The host supplies it.
**Parameters:** None documented on the trait.
**Returns:** Not stated.
**Errors:** Return `Err(String)` on send failure. It is surfaced as `FederationError::Transport`.
**Example:**
```rust
impl Transport for Host {
    // Send sealed frames to the peer endpoint. Return Err(String) on failure.
}
```

### `LocalDelivery`
**Signature:** `pub use crate::federation::LocalDelivery;`
**Purpose:** Where opened inbound envelopes are delivered locally (normally the switchboard).
**Parameters:** None documented on the trait.
**Returns:** Not stated.
**Errors:** Return `Err(String)` on failure. It is surfaced as `FederationError::Delivery`.
**Example:**
```rust
impl LocalDelivery for Host {
    // Deliver opened envelopes locally, normally by handing them to the Switchboard.
}
```

### `Peer`
**Signature:** `pub use crate::federation::Peer;`
**Purpose:** A registered remote node: its namespace, endpoint, shared key, and the provenance stamped on everything received from it.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
// A Peer: namespace, endpoint, PeerKey, and the Provenance stamped on what it sends.
```

### `PeerDirectory`
**Signature:** `pub use crate::federation::PeerDirectory;`
**Purpose:** The known peers, by namespace. `is_remote(ns)` decides between local and federated routing.
**Parameters:** `is_remote(ns)`: the namespace to check.
**Returns:** Not stated.
**Errors:** None.
**Example:**
```rust
if directory.is_remote("node-b") {
    // route through federation
}
```

### `PeerKey`
**Signature:** `pub use crate::federation::PeerKey;`
**Purpose:** A 32-byte pre-shared symmetric key for one peer link.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let key: PeerKey = /* 32-byte pre-shared key */;
```

### `Authorizer`
**Signature:** `pub use crate::federation::Authorizer;`
**Purpose:** Host policy at the federation seam. `authorize(from, to)` decides whether the edge-stamped (unforgeable) origin `from` may reach target `to`. It is called after stamping and before local delivery. Rejection fails closed.
**Parameters:** `authorize(from, ..)`: the authenticated origin address. `authorize(.., to)`: the target address.
**Returns:** Not stated.
**Errors:** Return `Err(reason)` to refuse. It is surfaced as `FederationError::Unauthorized`.
**Example:**
```rust
impl Authorizer for Policy {
    // authorize(from, to): return Err(reason) to refuse.
}
```

### `AllowAll`
**Signature:** `pub use crate::federation::AllowAll;`
**Purpose:** An `Authorizer` that admits every authenticated peer. It is meant for tests and trusted setups.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** None.
**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, AllowAll);
```

### `FederationError`
**Signature:** `pub use crate::federation::FederationError;`
**Purpose:** Everything that can go wrong at the federation boundary.
**Parameters:** None.
**Returns:** Nothing.
**Errors:** Not applicable; this is the error type itself.
**Example:**
```rust
match server.send(envelope) {
    Err(FederationError::Unauthorized { .. }) => { /* the Authorizer refused */ }
    _ => {}
}
```

### `seal`
**Signature:** `pub fn seal(envelope: &Envelope, sender: &str, key: &PeerKey) -> Result<Vec<u8>, FederationError>`
**Purpose:** Seals an envelope for a peer: it encodes the envelope and encrypts it with the peer's key.
**Parameters:** `envelope`: the envelope to send. `sender`: this node's namespace, which the key authenticates. `key`: the peer link's shared key.
**Returns:** The sealed frame bytes.
**Errors:** `FederationError::Codec` when encoding fails. `FederationError::Nonce` when no nonce can be generated. `FederationError::OpenFailed` when encryption fails.
**Example:**
```rust
let frame = seal(&envelope, "node-a", &key)?;
```

### `open`
**Signature:** `pub fn open(frame: &[u8], directory: &PeerDirectory) -> Result<(Envelope, String), FederationError>`
**Purpose:** Opens a sealed frame and authenticates the sender against the peer directory.
**Parameters:** `frame`: the received bytes. `directory`: the known peers and their keys.
**Returns:** The envelope and the authenticated sender namespace. The envelope is still untrusted.
**Errors:** `FederationError::UnknownPeer`, `MalformedFrame`, `UnsupportedVersion`, `UnsupportedAuth`, `OpenFailed` (wrong key or tampering), or `Codec`.
**Example:**
```rust
let (envelope, sender) = open(&frame, &directory)?;
```

## Changes since the last version
v1.0.0 (first version)

- Added: Pipeline, ListenerRegistry, ThreadRegistry, PipelineError, PipelineResult, Handler, FnHandler, HandlerContext, HandlerResponse, HandlerResult, ValidatedPayload, Middleware, DispatchMeta, PreDispatchVerdict, PostDispatchVerdict, DispatchObserver, CallKind, ThreadFailer, Envelope, Meta, Payload, PayloadValue, Field, Address, Segment, AddressError, Provenance, encode_envelope, decode_envelope, PayloadSchema, FieldSchema, FieldType, SchemaRegistry, validate_payload, Switchboard, Materializer, SwitchboardError, FederationServer, FederationEgress, Transport, LocalDelivery, Peer, PeerDirectory, PeerKey, Authorizer, AllowAll, FederationError, seal, open.
- Changed: none.
- Removed: none.

## Not part of the API
Everything outside the wrapper, `rust_pipeline::api` (and the prelude that re-exports it), stays inside the project and is not part of the API. The internal modules, including `codec`, `wire` and `pipeline`, are still `pub` so existing callers keep compiling. They are `#[doc(hidden)]` and may change without notice. Import only from `rust_pipeline::api` or the prelude.
