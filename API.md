# rust-pipeline API (v1.0.2)

## Overview
rust-pipeline is the public API of the rust-pipeline crate, and the only supported way other projects use it. Raw bytes are decoded, validated against a schema, routed, checked against peer rules, and dispatched to a handler. A handler's response is never trusted: it is re-encoded to bytes and runs through the same checks again. Nodes can also exchange sealed envelopes through the federation types.

## Getting started
```rust
use rust_pipeline::api::*;

// registry: a ListenerRegistry with listeners registered
// threads: a ThreadRegistry
let mut p = Pipeline::new(registry, threads);
p.run();
let raw = encode_envelope(&env)?;
p.inject(raw).await?;
p.shutdown().await;
```

## Reference

### `Pipeline`
**Signature:** `pub use crate::pipeline::Pipeline;`

**Purpose:** The self-feeding pipeline: four tokio stages joined by bounded channels. Build it with `Pipeline::new(registry, threads)` and install hooks (`add_middleware`, `with_dispatch_observer`, `with_federation`) before `run()`. `run()` needs `&mut` and must be called inside a tokio runtime. Feed it with `inject(raw_bytes)`, and stop it with `shutdown()`, which consumes the pipeline. `inject` and `shutdown` are `async`.

**Parameters:** `new(registry, threads)`: the listeners to dispatch to (`registry`) and the thread state to use (`threads`).

**Returns:** `inject` returns once the bytes are queued. `ingress_tx` and `thread_failer` hand out cloneable handles; call them after `run()`.

**Errors:** `inject` returns `PipelineError` when the pipeline is not running (ingress closed). Messages that fail parsing, validation, routing or peer checks are dropped and logged inside the pipeline. They never surface as errors to the injector.

**Example:**
```rust
use rust_pipeline::api::Pipeline;

let mut p = Pipeline::new(registry, threads);
p.run();
p.inject(raw_bytes).await?;
p.shutdown().await;
```

### `ListenerRegistry`
**Signature:** `pub use crate::registry::ListenerRegistry;`

**Purpose:** The set of listeners (handlers) a pipeline dispatches to. `register` adds one listener. The routing key is `name.payload_tag` in lowercase.

**Parameters:**
- `name`: the listener name, used as the address segment.
- `payload_tag`: the payload tag this listener accepts.
- `handler`: anything implementing `Handler`.
- `is_agent`: agents are peer-restricted and may only message `peers`.
- `peers`: a `Vec<String>` of names an agent may send to. `vec![]` means no one (deny by default).
- `description`: a human-readable purpose.
- `schema`: an optional `PayloadSchema`, enforced before dispatch.

**Returns:** nothing. The listener is stored in the registry.

**Errors:** none. Registration cannot fail. Registering the same name again replaces the handler.

**Example:**
```rust
registry.register("greeter", "greet", handler, false, vec![], "Greets callers", None);
```

### `ThreadRegistry`
**Signature:** `pub use crate::thread::ThreadRegistry;`

**Purpose:** Maps opaque thread UUIDs to dot-separated call chains (for example `console.router.greeter`), so UUIDs hide topology. Callers use `initialize_root`, `get_or_create`, `start_chain`, `extend_chain`, `prune_for_response`, `lookup` and `cleanup` to manage conversation threads.

**Parameters:** none stated in the doc comment.

**Returns:** methods that create threads return the new UUID. Lookups return `Option`.

**Errors:** none. None of its methods fail. An unknown thread yields `None` (or an empty provenance).

**Example:**
```rust
let root = threads.initialize_root(...); // returns the new thread UUID
```

### `PipelineError`
**Signature:** `pub use crate::error::PipelineError;`

**Purpose:** Every way the pipeline can fail: parse, validation, routing, peer violation, thread, handler and serialization errors. Handlers return it (via `HandlerResult`) to report failure. Callers match on it to tell, for example, `PeerViolation` from `NoRoute`.

**Parameters:** none.

**Returns:** nothing. It is an error type.

**Errors:** none. The failures it describes are listed in Purpose.

**Example:**
```rust
use rust_pipeline::api::PipelineError;

fn check(r: Result<(), PipelineError>) {
    if let Err(e) = r {
        // match on the variant, e.g. PeerViolation vs NoRoute
        let _ = e;
    }
}
```

### `PipelineResult`
**Signature:** `pub use crate::error::PipelineResult;`

**Purpose:** Shorthand for `Result<T, PipelineError>`, returned by every fallible pipeline call.

**Parameters:** none.

**Returns:** `Result<T, PipelineError>`.

**Errors:** `PipelineError` (see its entry).

**Example:**
```rust
use rust_pipeline::api::{decode_envelope, Envelope, PipelineResult};

fn load(raw: &[u8]) -> PipelineResult<Envelope> {
    decode_envelope(raw)
}
```

### `Handler`
**Signature:** `pub use crate::handler::Handler;`

**Purpose:** The trait every listener implements. `handle(payload, ctx)` receives an already validated, typed payload and returns what to do next. The response is never trusted: the pipeline re-encodes it to bytes and runs it through the full checks again.

**Parameters:**
- `payload`: the validated payload (`ValidatedPayload`).
- `ctx`: pipeline-verified metadata (`HandlerContext`).

**Returns:** a `HandlerResult` carrying the `HandlerResponse`.

**Errors:** return `Err(PipelineError::Handler(..))` to report failure. Panics are caught and surfaced as `PipelineError::HandlerPanic`.

**Example:**
```rust
impl Handler for Greeter {
    // handle(payload, ctx) -> HandlerResult
    // Ok(HandlerResponse::reply(payload.to_payload()))
}
```

### `FnHandler`
**Signature:** `pub use crate::handler::FnHandler;`

**Purpose:** Adapter that turns an async closure of the form `Fn(ValidatedPayload, HandlerContext) -> Pin<Box<Future<Output = HandlerResult>>>` into a `Handler`.

**Parameters:** `.0`: the closure.

**Returns:** a `Handler` built from the closure.

**Errors:** whatever the closure returns.

**Example:**
```rust
use rust_pipeline::api::{FnHandler, HandlerResponse};

let handler = FnHandler(|payload, ctx| Box::pin(async move {
    Ok(HandlerResponse::reply(payload.to_payload()))
}));
```

### `HandlerContext`
**Signature:** `pub use crate::handler::HandlerContext;`

**Purpose:** Verified metadata handed to a handler: the thread id, the sender address, its own address, and the message's provenance set. The pipeline sets it, and a handler cannot forge it. Provenance is observation-only, and a handler must not gate on it.

**Parameters:** none.

**Returns:** nothing. It is passed to `Handler::handle`.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::HandlerContext;

// Passed to Handler::handle(payload, ctx). Read it; don't gate on provenance.
```

### `HandlerResponse`
**Signature:** `pub use crate::handler::HandlerResponse;`

**Purpose:** What a handler wants to happen next.
- `Reply`: answer back up the thread chain.
- `Send`: synchronous sub-call. Suspend until the callee replies.
- `Spawn`: asynchronous delegation. Continue now, callback later.
- `None`: nothing. A waiting parent gets a synthesized ACK.

**Parameters:** the helpers are `reply(payload)`, `send(to, payload)` and `spawn(to, payload)`. `to` is the call target and `payload` is the payload to send.

**Returns:** a `HandlerResponse` value, one of the four variants.

**Errors:** a `Send` or `Spawn` to a non-peer is rejected by the pipeline with `PipelineError::PeerViolation`. This is not an error at construction time.

**Example:**
```rust
use rust_pipeline::api::HandlerResponse;

let answer = HandlerResponse::reply(payload);
let call = HandlerResponse::send(to, payload);
```

### `HandlerResult`
**Signature:** `pub use crate::handler::HandlerResult;`

**Purpose:** The return type of `Handler::handle`: `Result<HandlerResponse, PipelineError>`.

**Parameters:** none.

**Returns:** a `Result` holding a `HandlerResponse` or a `PipelineError`.

**Errors:** the `PipelineError` a handler returns to report failure (see `Handler`).

**Example:**
```rust
use rust_pipeline::api::{HandlerResponse, HandlerResult};

fn nothing_to_do() -> HandlerResult {
    Ok(HandlerResponse::None)
}
```

### `ValidatedPayload`
**Signature:** `pub use crate::handler::ValidatedPayload;`

**Purpose:** The payload a handler receives, after decode and schema validation. It holds the `tag` and a typed `PayloadValue`, never raw bytes. `to_payload()` clones it into an outgoing `Payload`.

**Parameters:** none.

**Returns:** `to_payload()` returns a `Payload`.

**Errors:** none.

**Example:**
```rust
let outgoing: Payload = payload.to_payload();
```

### `Middleware`
**Signature:** `pub use crate::middleware::Middleware;`

**Purpose:** Intercepts messages around dispatch (gates, guards, transforms). `pre_dispatch` runs in registration order and may continue, replace the payload, or short-circuit with its own response. `post_dispatch` runs in reverse order and may replace the handler's response. Both default to pass-through.

**Parameters:**
- `pre_dispatch(meta, ..., payload)`: the `DispatchMeta` for the message, and the payload.
- `post_dispatch(meta, ..., result)`: the `DispatchMeta` for the message, and the handler's result.

**Returns:** a `PreDispatchVerdict` or a `PostDispatchVerdict`.

**Errors:** none. Middleware does not fail. To block a message, short-circuit with a response.

**Example:**
```rust
impl Middleware for Gate {
    // pre_dispatch(meta, payload) -> PreDispatchVerdict
    // post_dispatch(meta, result) -> PostDispatchVerdict (default: pass-through)
}
```

### `DispatchMeta`
**Signature:** `pub use crate::middleware::DispatchMeta;`

**Purpose:** Verified facts about the message being dispatched (from, to, thread id, payload tag), as seen by middleware.

**Parameters:** none.

**Returns:** nothing. It is passed to middleware.

**Errors:** none.

**Example:**
```rust
// Given a DispatchMeta in pre_dispatch: from, to, thread id and payload tag.
```

### `PreDispatchVerdict`
**Signature:** `pub use crate::middleware::PreDispatchVerdict;`

**Purpose:** A middleware's decision before dispatch: continue, continue with a replaced payload, or short-circuit with a response.

**Parameters:** none.

**Returns:** the decision, as one of the three cases above.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::PreDispatchVerdict;

// Returned from Middleware::pre_dispatch: continue, continue with a replaced payload,
// or short-circuit with a response.
```

### `PostDispatchVerdict`
**Signature:** `pub use crate::middleware::PostDispatchVerdict;`

**Purpose:** A middleware's decision after dispatch: pass the response through or replace it.

**Parameters:** none.

**Returns:** the decision, either to pass the response through or to replace it.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::PostDispatchVerdict;

// Returned from Middleware::post_dispatch: pass the response through or replace it.
```

### `DispatchObserver`
**Signature:** `pub use crate::pipeline::DispatchObserver;`

**Purpose:** A host hook told when a call edge opens and closes. The host can set deadlines for hung `Send` and `Spawn` calls and keep an in-flight ledger. Install it with `Pipeline::with_dispatch_observer`. `on_open` and `on_close` run inline in dispatch and must be cheap.

**Parameters:**
- `on_open(caller, callee, thread, kind)`: the two ends of the edge, the thread the response is expected on, and the `CallKind`.
- `on_close(thread)`: the thread the response was expected on.

**Returns:** nothing. The callbacks return nothing.

**Errors:** none. The callbacks cannot fail the pipeline.

**Example:**
```rust
impl DispatchObserver for Ledger {
    // on_open(caller, callee, thread, kind)
    // on_close(thread)
}
```

### `CallKind`
**Signature:** `pub use crate::pipeline::CallKind;`

**Purpose:** Whether an in-flight call edge is synchronous or asynchronous.
- `CallKind::Sync`: opened by a `HandlerResponse::Send`. The caller is suspended until the reply.
- `CallKind::Async`: opened by a `HandlerResponse::Spawn`. The caller continues and receives a callback later.

**Parameters:** none.

**Returns:** nothing. It is passed to `DispatchObserver::on_open`.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::CallKind;

// CallKind::Sync  - opened by HandlerResponse::Send
// CallKind::Async - opened by HandlerResponse::Spawn
```

### `ThreadFailer`
**Signature:** `pub use crate::pipeline::ThreadFailer;`

**Purpose:** The recovery half of `DispatchObserver`. It injects a `Timeout` back to the caller waiting on a hung edge, and makes any late real reply drop. Get one from `Pipeline::thread_failer()` after `run()`.

**Parameters:** `fail(thread_id, reason)`: the hung edge's thread, and a human-readable reason.

**Returns:** `Ok(true)` if a waiting caller was failed. `Ok(false)` if the thread had none (already resolved, unknown or stale).

**Errors:** `PipelineError` when the `Timeout` cannot be encoded or the ingress is closed. The caller must have a route for the `Timeout` tag, or the injected failure is dropped.

**Example:**
```rust
let failed = failer.fail(thread_id, "no reply within deadline").await?;
// failed == true if a waiting caller was failed
```

### `Envelope`
**Signature:** `pub use crate::wire::Envelope;`

**Purpose:** The one canonical message: a `Meta` header plus a `Payload`.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::{encode_envelope, Envelope};

// env: an Envelope
let raw = encode_envelope(&env)?;
```

### `Meta`
**Signature:** `pub use crate::wire::Meta;`

**Purpose:** The envelope header: `from` (the last hop, for audit only), an optional `to`, the opaque `thread` id, and the durable `Provenance` set.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::Meta;

// The header of an Envelope. `to` is optional.
```

### `Payload`
**Signature:** `pub use crate::wire::Payload;`

**Purpose:** A typed payload: a `tag` (which selects the operation and schema) and a `PayloadValue`. `Payload::single(tag, field, text)` builds the common one-text-field case.

**Parameters:** `single(tag, field, text)`: `tag` is the payload tag, `field` is the field, and `text` is its text value.

**Returns:** a `Payload`.

**Errors:** none.

**Example:**
```rust
let p = Payload::single("greet", "name", "Ada");
```

### `PayloadValue`
**Signature:** `pub use crate::wire::PayloadValue;`

**Purpose:** A typed payload value: a record, sequence, text, integers, real, boolean, blob or nil. Helpers: `text`, `record`, `get(field)` and `as_text()`.

**Parameters:** `get(field)`: the field name.

**Returns:** the value kinds above. The return types of `get` and `as_text` are not stated in the doc comment.

**Errors:** none stated in the doc comment.

**Example:**
```rust
let name = value.get("name"); // accessor for a record field
```

### `Field`
**Signature:** `pub use crate::wire::Field;`

**Purpose:** One named field of a record `PayloadValue`.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::Field;

// A named field inside a record PayloadValue.
```

### `Address`
**Signature:** `pub use crate::wire::Address;`

**Purpose:** A hierarchical address, for example `agentos.concierge[alice]`. `Address::flat(name)` builds a one-segment address, and `Address::parse(s)` parses one. Accessors expose the target, organism, namespace, node, instance key, buffer and cache keys.

**Parameters:**
- `flat(name)`: the one-segment name.
- `parse(s)`: the address string.

**Returns:** `flat` returns an `Address`. `parse` returns the parsed address.

**Errors:** `Address::parse` returns `AddressError` for empty input, empty segments, unbalanced brackets or empty names.

**Example:**
```rust
use rust_pipeline::api::Address;

let a = Address::parse("agentos.concierge[alice]")?;
let b = Address::flat("concierge");
```

### `Segment`
**Signature:** `pub use crate::wire::Segment;`

**Purpose:** One segment of an `Address`: a name plus an optional instance key.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// In agentos.concierge[alice], the segments are "concierge" and "[alice]".
```

### `AddressError`
**Signature:** `pub use crate::wire::AddressError;`

**Purpose:** Why an address string failed to parse.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::{Address, AddressError};

let bad: Result<Address, AddressError> = Address::parse("");
```

### `Provenance`
**Signature:** `pub use crate::wire::Provenance;`

**Purpose:** The durable origin set, a 128-bit bit set, carried on every envelope and unioned downstream. rust-pipeline carries it. The host decides what each bit means and enforces policy. `Provenance::EMPTY` means *not cleared*, not *clean*.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
use rust_pipeline::api::Provenance;

let p = Provenance::EMPTY; // not cleared, not clean
```

### `encode_envelope`
**Signature:** `pub fn encode_envelope(env: &Envelope) -> PipelineResult<Vec<u8>>`

**Purpose:** Encodes a typed envelope to wire bytes. This is the only way to produce bytes to inject.

**Parameters:** `env`: the envelope to encode.

**Returns:** the wire bytes (XML today, transitional).

**Errors:** `PipelineError::Serialization` when the envelope cannot be written.

**Example:**
```rust
let raw = encode_envelope(&env)?;
p.inject(raw).await?;
```

### `decode_envelope`
**Signature:** `pub fn decode_envelope(raw: &[u8]) -> PipelineResult<Envelope>`

**Purpose:** Decodes wire bytes into a typed envelope. Missing provenance decodes as the empty set. A missing `to` decodes as `None`.

**Parameters:** `raw`: untrusted wire bytes.

**Returns:** the decoded `Envelope`.

**Errors:** `PipelineError::EnvelopeParse`, `MissingMeta` or `Xml` when the bytes are not a valid envelope.

**Example:**
```rust
let env = decode_envelope(&raw)?;
```

### `PayloadSchema`
**Signature:** `pub use crate::validation::PayloadSchema;`

**Purpose:** The expected shape of a payload: its root tag, its fields, and whether unknown fields are rejected (`strict`).

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// Pass as the schema to ListenerRegistry::register, or to validate_payload.
```

### `FieldSchema`
**Signature:** `pub use crate::validation::FieldSchema;`

**Purpose:** One field of a `PayloadSchema`: whether it is required, and its `FieldType`.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// One entry per field: a required flag and a FieldType.
```

### `FieldType`
**Signature:** `pub use crate::validation::FieldType;`

**Purpose:** A field's type: string, integer, boolean, nested schema, or any.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// The type of a FieldSchema: string, integer, boolean, nested schema, or any.
```

### `SchemaRegistry`
**Signature:** `pub use crate::validation::SchemaRegistry;`

**Purpose:** Schemas keyed by payload tag. `validate(tag, bytes)` and `validate_value(tag, value)` check a payload against the registered schema.

**Parameters:**
- `validate(tag, bytes)`: the payload tag and its bytes.
- `validate_value(tag, value)`: the payload tag and its value.

**Returns:** not stated in the doc comment.

**Errors:** `PipelineError::Validation` when the payload does not match.

**Example:**
```rust
schemas.validate("greet", bytes)?;
```

### `validate_payload`
**Signature:** `pub fn validate_payload(payload_xml: &[u8], schema: &PayloadSchema) -> PipelineResult<()>`

**Purpose:** Checks a payload's bytes against a schema, before or outside the pipeline.

**Parameters:**
- `payload_xml`: the payload element's bytes.
- `schema`: the schema to check against.

**Returns:** `Ok(())` when the payload matches.

**Errors:** `PipelineError::Validation` describing the first mismatch (wrong root, missing required field, or unexpected field under `strict`). `PipelineError::Xml` for malformed bytes.

**Example:**
```rust
validate_payload(&xml, &schema)?;
```

### `Switchboard`
**Signature:** `pub use crate::switchboard::Switchboard;`

**Purpose:** A materialize-then-inject front over a pipeline's ingress. `route(envelope)` resolves the destination instance, stamps its thread and injects. `route` is `async`.

**Parameters:**
- `new(materializer, ingress)`: a `Materializer`, and the pipeline's ingress handle (`pipeline.ingress_tx()`).
- `route(envelope)`: an envelope whose `meta.to` names the destination.

**Returns:** not stated in the doc comment for `route`.

**Errors:** `SwitchboardError` when there is no destination, materialization fails, the ingress is closed, or encoding fails. Nothing is injected on failure (fail-closed).

**Example:**
```rust
let switchboard = Switchboard::new(materializer, pipeline.ingress_tx());
switchboard.route(envelope).await?;
```

### `Materializer`
**Signature:** `pub use crate::switchboard::Materializer;`

**Purpose:** A host hook that resolves an instance address to its delivery thread, creating the instance on first use. `evict` releases it.

**Parameters:** `resolve(address)`: the instance address, and the optional buffer address.

**Returns:** the delivery thread id.

**Errors:** return `Err(String)` to refuse. The switchboard reports it as `SwitchboardError::Materialize`.

**Example:**
```rust
impl Materializer for Hosts {
    // resolve(address) -> delivery thread id
    // evict(...) releases the instance
}
```

### `SwitchboardError`
**Signature:** `pub use crate::switchboard::SwitchboardError;`

**Purpose:** Why `Switchboard::route` failed. Variants:
- `NoDestination`: there is no `meta.to`.
- `Materialize(String)`: the `Materializer` refused.
- `IngressClosed`.
- `Encode(PipelineError)`.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
match err {
    SwitchboardError::Materialize(reason) => eprintln!("refused: {reason}"),
    _ => {}
}
```

### `FederationServer`
**Signature:** `pub use crate::federation::FederationServer;`

**Purpose:** The node-boundary server. It seals and sends envelopes to remote nodes, and opens, stamps and locally delivers inbound frames.

**Parameters:**
- `new(node, directory, transport, local, authorizer)`: `node` is this node's namespace. `directory` holds the known peers. `transport` moves sealed frames (`Transport`). `local` is where opened envelopes go (`LocalDelivery`). `authorizer` decides whether an authenticated origin may reach its target (`Authorizer`).
- `send(envelope)`: an envelope addressed to a registered remote node.
- `receive(frame)`: a sealed frame from the transport.
- `run_egress(rx)`: drains the pipeline's federation-egress channel.

**Returns:** not stated in the doc comment.

**Errors:** `send` and `receive` return `FederationError` (unknown peer, bad frame, failed authentication, unauthorized, transport or delivery failure).

**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, authorizer);
server.send(envelope).await?;
```

### `FederationEgress`
**Signature:** `pub use crate::federation::FederationEgress;`

**Purpose:** The egress hook installed with `Pipeline::with_federation`. Messages addressed to a registered remote node go here instead of to a local listener.

**Parameters:** none stated in the doc comment.

**Returns:** none stated in the doc comment.

**Errors:** none stated in the doc comment.

**Example:**
```rust
// Installed with Pipeline::with_federation. Remote-addressed messages are handed here.
```

### `Transport`
**Signature:** `pub use crate::federation::Transport;`

**Purpose:** Moves sealed frames to a peer endpoint (TCP, websocket, and so on). The host supplies it.

**Parameters:** none stated in the doc comment.

**Returns:** none stated in the doc comment.

**Errors:** return `Err(String)` on send failure. This is surfaced as `FederationError::Transport`.

**Example:**
```rust
impl Transport for Wire {
    // send a sealed frame; return Err(String) on failure
}
```

### `LocalDelivery`
**Signature:** `pub use crate::federation::LocalDelivery;`

**Purpose:** Where opened inbound envelopes are delivered locally, normally the switchboard.

**Parameters:** none stated in the doc comment.

**Returns:** none stated in the doc comment.

**Errors:** return `Err(String)` on failure. This is surfaced as `FederationError::Delivery`.

**Example:**
```rust
// Implement LocalDelivery to hand opened envelopes to the switchboard.
```

### `Peer`
**Signature:** `pub use crate::federation::Peer;`

**Purpose:** A registered remote node: its namespace, endpoint, shared key, and the provenance stamped on everything received from it.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// A remote node entry: namespace, endpoint, shared key and stamped provenance.
```

### `PeerDirectory`
**Signature:** `pub use crate::federation::PeerDirectory;`

**Purpose:** The known peers, by namespace. `is_remote(ns)` decides between local and federated routing.

**Parameters:** `is_remote(ns)`: the namespace to check.

**Returns:** the decision from `is_remote`. The return type is not stated in the doc comment.

**Errors:** none.

**Example:**
```rust
let remote = directory.is_remote("node-b");
```

### `PeerKey`
**Signature:** `pub use crate::federation::PeerKey;`

**Purpose:** A 32-byte pre-shared symmetric key for one peer link.

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
// A 32-byte key shared with one peer.
```

### `Authorizer`
**Signature:** `pub use crate::federation::Authorizer;`

**Purpose:** Host policy at the federation seam. `authorize(from, to)` decides whether the edge-stamped (unforgeable) origin `from` may reach target `to`. It is called after stamping and before local delivery. Rejection fails closed.

**Parameters:**
- `authorize(from, ..)`: the authenticated origin address.
- `authorize(.., to)`: the target address.

**Returns:** not stated in the doc comment. Return `Err(reason)` to refuse.

**Errors:** return `Err(reason)` to refuse. This is surfaced as `FederationError::Unauthorized`.

**Example:**
```rust
impl Authorizer for Policy {
    // authorize(from, to): Err(reason) refuses; rejection fails closed
}
```

### `AllowAll`
**Signature:** `pub use crate::federation::AllowAll;`

**Purpose:** An `Authorizer` that admits every authenticated peer. Intended for tests and trusted setups.

**Parameters:** none.

**Returns:** not stated in the doc comment.

**Errors:** none.

**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, AllowAll);
```

### `FederationError`
**Signature:** `pub use crate::federation::FederationError;`

**Purpose:** Everything that can go wrong at the federation boundary. Variants:
- `UnknownPeer(String)`
- `MalformedFrame`
- `UnsupportedVersion(u8)`
- `UnsupportedAuth(u8)`
- `OpenFailed`
- `Nonce`
- `NoDestination`
- `NotRemote(String)`
- `Transport(String)`
- `Delivery(String)`
- `Unauthorized(String)`
- `Codec(PipelineError)`

**Parameters:** none.

**Returns:** nothing.

**Errors:** none.

**Example:**
```rust
match err {
    FederationError::Unauthorized(reason) => eprintln!("refused: {reason}"),
    _ => {}
}
```

### `seal`
**Signature:** `pub fn seal(envelope: &Envelope, sender: &str, key: &PeerKey) -> Result<Vec<u8>, FederationError>`

**Purpose:** Seals an envelope for a peer: it encodes the envelope and encrypts it with the peer's key.

**Parameters:**
- `envelope`: the envelope to send.
- `sender`: this node's namespace, which the key authenticates.
- `key`: the peer link's shared key.

**Returns:** the sealed frame bytes.

**Errors:** `FederationError::Codec` when encoding fails. `FederationError::Nonce` when no nonce can be generated. `FederationError::OpenFailed` when encryption fails.

**Example:**
```rust
let frame = seal(&env, "node-a", &key)?;
```

### `open`
**Signature:** `pub fn open(frame: &[u8], directory: &PeerDirectory) -> Result<(Envelope, String), FederationError>`

**Purpose:** Opens a sealed frame and authenticates the sender against the peer directory.

**Parameters:**
- `frame`: the received bytes.
- `directory`: the known peers and their keys.

**Returns:** the envelope and the authenticated sender namespace. The envelope is still untrusted.

**Errors:** `FederationError::UnknownPeer`, `MalformedFrame`, `UnsupportedVersion`, `UnsupportedAuth`, `OpenFailed` (wrong key or tampering), or `Codec`.

**Example:**
```rust
let (env, sender) = open(&frame, &directory)?;
// env is still untrusted
```

## Changes since the last version
v1.0.2 (docs only). No signatures or behaviour changed.

- Added: none.
- Changed: none.
- Removed: none.
- Docs changed: `ListenerRegistry`.

## Not part of the API
Everything outside the wrapper stays inside the crate. Callers should import from `rust_pipeline::api` (or the prelude, which re-exports it), and not from the internal modules `codec`, `wire` or `pipeline`, or any other module. Those modules are still `pub` so existing callers keep compiling, but they are `#[doc(hidden)]` and may change without notice.
