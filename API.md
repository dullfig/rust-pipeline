# rust-pipeline API (v1.0.1)

## Overview
rust-pipeline is a message pipeline for routing typed, validated messages between listeners (handlers), with optional middleware, thread tracking, and federation between nodes. Incoming raw bytes pass through decode, schema validation, routing, peer enforcement, and then dispatch to a handler. A handler's response is re-encoded to bytes and runs through the same checks again, because responses are never trusted. The wrapper module, `rust_pipeline::api`, is the only supported entry point. Every item in it is a re-export or a one-line wrapper.

## Getting started
```rust
use rust_pipeline::api::*;
```

```rust
// `registry` is a ListenerRegistry with listeners registered, and `threads` is the
// thread state to use. Run inside a tokio runtime.
let mut pipeline = Pipeline::new(registry, threads);
pipeline.run();
let bytes = encode_envelope(&envelope)?; // `envelope` is an Envelope
pipeline.inject(bytes).await?;
pipeline.shutdown().await;
```

## Reference

### `Pipeline`
**Signature:** `pub use crate::pipeline::Pipeline;`
**Purpose:** The self-feeding pipeline: four tokio stages joined by bounded channels. Build it with `Pipeline::new(registry, threads)`. Install hooks (`add_middleware`, `with_dispatch_observer`, `with_federation`) before `run()`, which needs `&mut` and must be called inside a tokio runtime. Feed it with `inject(raw_bytes).await` and stop it with `shutdown().await`, which consumes the pipeline. `inject` and `shutdown` are `async`.
**Parameters:** `new(registry, threads)`: the listeners to dispatch to, and the thread state to use.
**Returns:** `inject` returns once the bytes are queued. `ingress_tx` and `thread_failer` hand out clonable handles; call them after `run()`.
**Errors:** `inject` returns `PipelineError` when the pipeline is not running (ingress closed). Messages that fail parsing, validation, routing or peer checks are dropped and logged inside the pipeline. They never surface as errors to the injector.
**Example:**
```rust
let mut pipeline = Pipeline::new(registry, threads);
// install hooks here, before run()
pipeline.run();
pipeline.inject(encode_envelope(&envelope)?).await?;
pipeline.shutdown().await;
```

### `ListenerRegistry`
**Signature:** `pub use crate::registry::ListenerRegistry;`
**Purpose:** The set of listeners (handlers) a pipeline dispatches to. `register(...)` adds one listener. The routing key is `name.payload_tag` (lowercase).
**Parameters:**
- `name`: listener name, used as the address segment.
- `payload_tag`: the payload tag this listener accepts.
- `handler`: anything implementing `Handler`.
- `is_agent`: agents are peer-restricted and may only message `peers`.
- `peers`: names an agent may send to. Empty means no one (deny by default).
- `description`: human-readable purpose.
- `schema`: optional `PayloadSchema`, enforced before dispatch.

**Returns:** Not stated in the doc comment.
**Errors:** Registration cannot fail. Registering the same name again replaces the handler.
**Example:**
```rust
registry.register(
    "greeter",          // name
    "greet",            // payload_tag; routing key is greeter.greet
    handler,            // anything implementing Handler
    false,              // is_agent
    &[],                // peers (empty; only agents use it)
    "Greets callers",   // description
    None,               // schema
);
```

### `ThreadRegistry`
**Signature:** `pub use crate::thread::ThreadRegistry;`
**Purpose:** Maps opaque thread UUIDs to dot-separated call chains (e.g. `console.router.greeter`), so UUIDs hide topology. Callers use `initialize_root`, `get_or_create`, `start_chain`, `extend_chain`, `prune_for_response`, `lookup` and `cleanup` to manage conversation threads.
**Parameters:** none
**Returns:** Methods that create threads return the new UUID. Lookups return `Option`.
**Errors:** None of its methods fail. An unknown thread yields `None` (or an empty provenance).
**Example:**
```rust
// `threads` is a ThreadRegistry; `uuid` is an opaque thread id.
if threads.lookup(uuid).is_none() {
    // unknown thread
}
```

### `PipelineError`
**Signature:** `pub use crate::error::PipelineError;`
**Purpose:** Every way the pipeline can fail: parse, validation, routing, peer violation, thread, handler and serialization errors. Handlers return it (via `HandlerResult`) to report failure. Callers match on it to tell, for example, `PeerViolation` from `NoRoute`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
let result: PipelineResult<()> = pipeline.inject(bytes).await;
if let Err(e) = result {
    // e is a PipelineError; match on it to tell failures apart
}
```

### `PipelineResult`
**Signature:** `pub use crate::error::PipelineResult;`
**Purpose:** Shorthand for `Result<T, PipelineError>`, returned by every fallible pipeline call.
**Parameters:** none
**Returns:** nothing (a type alias)
**Errors:** none
**Example:**
```rust
let bytes: PipelineResult<Vec<u8>> = encode_envelope(&envelope);
```

### `Handler`
**Signature:** `pub use crate::handler::Handler;`
**Purpose:** The trait every listener implements. `handle(payload, ctx)` receives an already validated, typed payload and returns what to do next. The response is never trusted: the pipeline re-encodes it to bytes and runs it through the full checks again.
**Parameters:**
- `handle(payload, ..)`: the validated payload (`ValidatedPayload`).
- `handle(.., ctx)`: pipeline-verified metadata (`HandlerContext`).

**Returns:** A `HandlerResult` carrying the `HandlerResponse`.
**Errors:** Return `Err(PipelineError::Handler(..))` to report failure. Panics are caught and surfaced as `PipelineError::HandlerPanic`.
**Example:**
```rust
// Body of Handler::handle for a listener that echoes its input:
Ok(HandlerResponse::reply(payload.to_payload()))
```

### `FnHandler`
**Signature:** `pub use crate::handler::FnHandler;`
**Purpose:** Adapter that turns an async closure of the form `Fn(ValidatedPayload, HandlerContext) -> Pin<Box<Future<Output = HandlerResult>>>` into a `Handler`.
**Parameters:** `.0`: the closure.
**Returns:** Not stated in the doc comment.
**Errors:** Whatever the closure returns.
**Example:**
```rust
let handler = FnHandler(|payload, ctx| Box::pin(async move {
    Ok(HandlerResponse::None)
}));
```

### `HandlerContext`
**Signature:** `pub use crate::handler::HandlerContext;`
**Purpose:** Verified metadata handed to a handler: the thread id, the sender address, the handler's own address, and the message's provenance set. The pipeline sets it, and a handler cannot forge it. Provenance is observation-only, so a handler must not gate on it.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Inside Handler::handle, `ctx` is pipeline-verified.
// Read it for observation only; do not gate on provenance.
```

### `HandlerResponse`
**Signature:** `pub use crate::handler::HandlerResponse;`
**Purpose:** What a handler wants to happen next.
- `Reply`: answer back up the thread chain.
- `Send`: synchronous sub-call. Suspends until the callee replies.
- `Spawn`: asynchronous delegation. Continues now, with a callback later.
- `None`: nothing. A waiting parent gets a synthesized ACK.

Helpers: `HandlerResponse::reply(payload)`, `::send(to, payload)`, `::spawn(to, payload)`.

**Parameters:** The helpers take `reply(payload)`, `send(to, payload)` and `spawn(to, payload)`. The doc comment does not describe them further.
**Returns:** Not stated in the doc comment.
**Errors:** A `Send` or `Spawn` to a non-peer is rejected by the pipeline with `PipelineError::PeerViolation`. It is not an error at construction time.
**Example:**
```rust
let answer = HandlerResponse::reply(payload.to_payload());
let call = HandlerResponse::send(to, payload.to_payload());   // waits for the reply
let delegate = HandlerResponse::spawn(to, payload.to_payload()); // callback later
let nothing = HandlerResponse::None;
```

### `HandlerResult`
**Signature:** `pub use crate::handler::HandlerResult;`
**Purpose:** The return type of `Handler::handle`: `Result<HandlerResponse, PipelineError>`.
**Parameters:** none
**Returns:** A `HandlerResponse`, or a `PipelineError`.
**Errors:** none
**Example:**
```rust
fn finish() -> HandlerResult {
    Ok(HandlerResponse::None)
}
```

### `ValidatedPayload`
**Signature:** `pub use crate::handler::ValidatedPayload;`
**Purpose:** The payload a handler receives after decode and schema validation. It holds the `tag` and a typed `PayloadValue`, never raw bytes.
**Parameters:** none
**Returns:** nothing. `to_payload()` returns a clone as an outgoing `Payload`.
**Errors:** none
**Example:**
```rust
let outgoing: Payload = payload.to_payload();
```

### `Middleware`
**Signature:** `pub use crate::middleware::Middleware;`
**Purpose:** Intercepts messages around dispatch (gates, guards, transforms). `pre_dispatch` runs in registration order and may continue, replace the payload, or short-circuit with its own response. `post_dispatch` runs in reverse order and may replace the handler's response. Both default to pass-through.
**Parameters:**
- `pre_dispatch(meta, ..)` and `post_dispatch(meta, ..)`: the `DispatchMeta` for the message.
- `pre_dispatch(.., payload)`: the payload.
- `post_dispatch(.., result)`: the handler's result.

**Returns:** A `PreDispatchVerdict` or a `PostDispatchVerdict`.
**Errors:** Middleware does not fail. To block a message, short-circuit with a response.
**Example:**
```rust
// Implement Middleware, overriding only the hooks you need.
// pre_dispatch returns a PreDispatchVerdict; post_dispatch returns a PostDispatchVerdict.
```

### `DispatchMeta`
**Signature:** `pub use crate::middleware::DispatchMeta;`
**Purpose:** Verified facts about the message being dispatched (from, to, thread id, payload tag), as seen by middleware.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// In Middleware::pre_dispatch(meta, payload), `meta` carries the verified
// from, to, thread id and payload tag of the message.
```

### `PreDispatchVerdict`
**Signature:** `pub use crate::middleware::PreDispatchVerdict;`
**Purpose:** A middleware's decision before dispatch: continue, continue with a replaced payload, or short-circuit with a response.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Returned from Middleware::pre_dispatch to continue, replace the payload,
// or short-circuit with a response.
```

### `PostDispatchVerdict`
**Signature:** `pub use crate::middleware::PostDispatchVerdict;`
**Purpose:** A middleware's decision after dispatch: pass the response through, or replace it.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Returned from Middleware::post_dispatch to pass the response through or replace it.
```

### `DispatchObserver`
**Signature:** `pub use crate::pipeline::DispatchObserver;`
**Purpose:** Host hook told when a call edge opens and closes, so the host can set deadlines for hung `Send` and `Spawn` calls and keep an in-flight ledger. Install it with `Pipeline::with_dispatch_observer`. `on_open` and `on_close` run inline in dispatch and must be cheap.
**Parameters:**
- `on_open(caller, callee, ..)`: the two ends of the edge.
- `on_open(.., thread, ..)` and `on_close(thread)`: the thread the response is expected on.
- `on_open(.., kind)`: the `CallKind`.

**Returns:** nothing. The callbacks return nothing.
**Errors:** The callbacks cannot fail the pipeline.
**Example:**
```rust
let mut pipeline = Pipeline::new(registry, threads);
// pipeline.with_dispatch_observer(observer); // before run()
```

### `CallKind`
**Signature:** `pub use crate::pipeline::CallKind;`
**Purpose:** Whether an in-flight call edge is synchronous or asynchronous.
- `CallKind::Sync`: opened by a `HandlerResponse::Send`. The caller is suspended until the reply.
- `CallKind::Async`: opened by a `HandlerResponse::Spawn`. The caller continues and receives a callback later.

**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
let sync_kind = CallKind::Sync;       // opened by HandlerResponse::send
let async_kind = CallKind::Async;     // opened by HandlerResponse::spawn
```

### `ThreadFailer`
**Signature:** `pub use crate::pipeline::ThreadFailer;`
**Purpose:** The recovery half of `DispatchObserver`. It injects a `Timeout` back to the caller waiting on a hung edge, and any late real reply then drops. Get one from `Pipeline::thread_failer()` after `run()`.
**Parameters:** `fail(thread_id, reason)`: the hung edge's thread, and a human-readable reason. The method is `async`.
**Returns:** `Ok(true)` if a waiting caller was failed. `Ok(false)` if the thread had none (already resolved, unknown or stale).
**Errors:** `PipelineError` when the `Timeout` cannot be encoded or the ingress is closed. The caller must have a route for the `Timeout` tag, or the injected failure is dropped.
**Example:**
```rust
let failer = pipeline.thread_failer(); // after run()
let failed = failer.fail(thread_id, "no reply before deadline").await?;
```

### `Envelope`
**Signature:** `pub use crate::wire::Envelope;`
**Purpose:** The one canonical message: a `Meta` header plus a `Payload`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// `envelope` is an Envelope built from a Meta header and a Payload.
let bytes = encode_envelope(&envelope)?;
```

### `Meta`
**Signature:** `pub use crate::wire::Meta;`
**Purpose:** The envelope header. It holds `from` (the last hop, for audit only), an optional `to`, the opaque `thread` id, and the durable `Provenance` set.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Meta carries: from (last hop, audit only), to (optional),
// thread (opaque id), and the Provenance set.
```

### `Payload`
**Signature:** `pub use crate::wire::Payload;`
**Purpose:** A typed payload: a `tag` (selects the operation and schema) and a `PayloadValue`. `Payload::single(tag, field, text)` builds the common case of one text field.
**Parameters:** none stated as an Arguments section. `single` takes a tag, a field name and text.
**Returns:** nothing
**Errors:** none
**Example:**
```rust
let greet = Payload::single("greet", "name", "Ada");
```

### `PayloadValue`
**Signature:** `pub use crate::wire::PayloadValue;`
**Purpose:** A typed payload value: a record, sequence, text, integers, real, boolean, blob, or nil. Helpers: `text`, `record`, `get(field)`, `as_text()`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// `value` is a record PayloadValue.
let name = value.get("name");
```

### `Field`
**Signature:** `pub use crate::wire::Field;`
**Purpose:** One named field of a record `PayloadValue`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// A record PayloadValue holds Fields. Read one with get(field name).
```

### `Address`
**Signature:** `pub use crate::wire::Address;`
**Purpose:** A hierarchical address, e.g. `agentos.concierge[alice]`. `Address::flat(name)` builds a one-segment address. `Address::parse(s)` parses one. Accessors expose the target, organism, namespace, node, instance key, buffer and cache keys.
**Parameters:** none stated as an Arguments section. `flat` takes a name, and `parse` takes an address string.
**Returns:** `Address::flat` builds a one-segment address. `Address::parse` returns the parsed address.
**Errors:** `Address::parse` returns `AddressError` for empty input, empty segments, unbalanced brackets, or empty names.
**Example:**
```rust
let flat = Address::flat("concierge");
let parsed = Address::parse("agentos.concierge[alice]")?;
```

### `Segment`
**Signature:** `pub use crate::wire::Segment;`
**Purpose:** One segment of an `Address`: a name plus an optional instance key.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Segments make up an Address, e.g. the concierge[alice] part of agentos.concierge[alice].
```

### `AddressError`
**Signature:** `pub use crate::wire::AddressError;`
**Purpose:** Why an address string failed to parse.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
match Address::parse(s) {
    Ok(address) => { /* use address */ }
    Err(e) => { /* e is an AddressError */ }
}
```

### `Provenance`
**Signature:** `pub use crate::wire::Provenance;`
**Purpose:** The durable origin set, a 128-bit bit set, carried on every envelope and unioned downstream. rust-pipeline carries it, and the host decides what each bit means and enforces policy. `Provenance::EMPTY` means *not cleared*, not *clean*.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
let none_recorded = Provenance::EMPTY; // means "not cleared", not "clean"
```

### `encode_envelope`
**Signature:** `pub fn encode_envelope(env: &Envelope) -> PipelineResult<Vec<u8>>`
**Purpose:** Encodes a typed envelope to wire bytes. This is the only way to produce bytes to inject.
**Parameters:** `env`: the envelope to encode.
**Returns:** The wire bytes (XML today, transitional).
**Errors:** `PipelineError::Serialization` when the envelope cannot be written.
**Example:**
```rust
let bytes = encode_envelope(&envelope)?;
pipeline.inject(bytes).await?;
```

### `decode_envelope`
**Signature:** `pub fn decode_envelope(raw: &[u8]) -> PipelineResult<Envelope>`
**Purpose:** Decodes wire bytes into a typed envelope. Missing provenance decodes as the empty set. A missing `to` decodes as `None`.
**Parameters:** `raw`: untrusted wire bytes.
**Returns:** The decoded `Envelope`.
**Errors:** `PipelineError::EnvelopeParse`, `MissingMeta` or `Xml` when the bytes are not a valid envelope.
**Example:**
```rust
let envelope = decode_envelope(&raw)?; // `raw` is untrusted wire bytes
```

### `PayloadSchema`
**Signature:** `pub use crate::validation::PayloadSchema;`
**Purpose:** The expected shape of a payload: its root tag, its fields, and whether unknown fields are rejected (`strict`).
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// A PayloadSchema sets the root tag, the FieldSchemas, and whether strict rejects unknown fields.
```

### `FieldSchema`
**Signature:** `pub use crate::validation::FieldSchema;`
**Purpose:** One field of a `PayloadSchema`: whether it is required, and its `FieldType`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Each FieldSchema says whether its field is required and what FieldType it has.
```

### `FieldType`
**Signature:** `pub use crate::validation::FieldType;`
**Purpose:** A field's type: string, integer, boolean, nested schema, or any.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// A FieldSchema's FieldType is one of: string, integer, boolean, nested schema, or any.
```

### `SchemaRegistry`
**Signature:** `pub use crate::validation::SchemaRegistry;`
**Purpose:** Holds schemas keyed by payload tag. `validate(tag, bytes)` and `validate_value(tag, value)` check a payload against the registered schema.
**Parameters:** `validate(tag, bytes)`: the payload tag and its bytes. `validate_value(tag, value)`: the payload tag and its value.
**Returns:** Not stated in the doc comment.
**Errors:** `PipelineError::Validation` when the payload does not match.
**Example:**
```rust
// `schemas` is a SchemaRegistry.
schemas.validate("greet", bytes)?;
```

### `validate_payload`
**Signature:** `pub fn validate_payload(payload_xml: &[u8], schema: &PayloadSchema) -> PipelineResult<()>`
**Purpose:** Checks a payload's bytes against a schema, before or outside the pipeline.
**Parameters:**
- `payload_xml`: the payload element's bytes.
- `schema`: the schema to check against.

**Returns:** `Ok(())` when the payload matches.
**Errors:** `PipelineError::Validation` describing the first mismatch (wrong root, missing required field, unexpected field under `strict`). `PipelineError::Xml` for malformed bytes.
**Example:**
```rust
validate_payload(payload_bytes, &schema)?; // `schema` is a PayloadSchema
```

### `Switchboard`
**Signature:** `pub use crate::switchboard::Switchboard;`
**Purpose:** A materialize-then-inject front over a pipeline's ingress. It is built with `Switchboard::new(materializer, pipeline.ingress_tx())`. `route(envelope)` resolves the destination instance, stamps its thread, and injects. `route` is `async`.
**Parameters:**
- `new(materializer, pipeline.ingress_tx())`: the `Materializer`, and the pipeline's ingress.
- `route(envelope)`: an envelope whose `meta.to` names the destination.

**Returns:** Not stated in the doc comment.
**Errors:** `SwitchboardError` when there is no destination, materialization fails, the ingress is closed, or encoding fails. Nothing is injected on failure (fail-closed).
**Example:**
```rust
let switchboard = Switchboard::new(materializer, pipeline.ingress_tx());
switchboard.route(envelope).await?;
```

### `Materializer`
**Signature:** `pub use crate::switchboard::Materializer;`
**Purpose:** Host hook that resolves an instance address to its delivery thread, creating the instance on first use. `evict` releases it.
**Parameters:** `resolve(address)`: the instance address, and the optional buffer address.
**Returns:** The delivery thread id.
**Errors:** Return `Err(String)` to refuse. The switchboard reports it as `SwitchboardError::Materialize`.
**Example:**
```rust
// Implement Materializer::resolve(address): return the delivery thread id,
// or Err(String) to refuse.
```

### `SwitchboardError`
**Signature:** `pub use crate::switchboard::SwitchboardError;`
**Purpose:** Why `Switchboard::route` failed. Variants: `NoDestination` (no `meta.to`), `Materialize(String)` (the `Materializer` refused), `IngressClosed`, and `Encode(PipelineError)`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
match switchboard.route(envelope).await {
    Err(SwitchboardError::NoDestination) => { /* envelope has no meta.to */ }
    Err(SwitchboardError::Materialize(reason)) => { /* the Materializer refused */ }
    _ => {}
}
```

### `FederationServer`
**Signature:** `pub use crate::federation::FederationServer;`
**Purpose:** The node-boundary server. It seals and sends envelopes to remote nodes, and opens, stamps and locally delivers inbound frames. It is built with `FederationServer::new(node, directory, transport, local, authorizer)`. Then use `send(envelope).await`, `receive(frame).await`, and `run_egress(rx).await` to drain the pipeline's federation-egress channel. All three are `async`.
**Parameters:**
- `new(node, ..)`: this node's namespace.
- `new(.., directory, ..)`: the known peers.
- `new(.., transport, ..)`: moves sealed frames (`Transport`).
- `new(.., local, ..)`: where opened envelopes go (`LocalDelivery`).
- `new(.., authorizer)`: decides whether an authenticated origin may reach its target (`Authorizer`).
- `send(envelope)`: an envelope addressed to a registered remote node.
- `receive(frame)`: a sealed frame from the transport.

**Returns:** Not stated in the doc comment.
**Errors:** `send` and `receive` return `FederationError`, for an unknown peer, bad frame, failed authentication, unauthorized origin, or transport or delivery failure.
**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, authorizer);
server.send(envelope).await?;   // to a registered remote node
server.receive(frame).await?;   // a sealed frame from the transport
```

### `FederationEgress`
**Signature:** `pub use crate::federation::FederationEgress;`
**Purpose:** The egress hook installed with `Pipeline::with_federation`. Messages addressed to a registered remote node go here instead of to a local listener.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// Installed with pipeline.with_federation(...). Messages for remote nodes go to it,
// not to local listeners.
```

### `Transport`
**Signature:** `pub use crate::federation::Transport;`
**Purpose:** Moves sealed frames to a peer endpoint (TCP, websocket, and so on). The host supplies it.
**Parameters:** none
**Returns:** Not stated in the doc comment.
**Errors:** Return `Err(String)` on send failure. It is surfaced as `FederationError::Transport`.
**Example:**
```rust
// Implement Transport to move sealed frames to a peer endpoint.
// Return Err(String) on send failure.
```

### `LocalDelivery`
**Signature:** `pub use crate::federation::LocalDelivery;`
**Purpose:** Where opened inbound envelopes are delivered locally. This is normally the switchboard.
**Parameters:** none
**Returns:** nothing
**Errors:** Return `Err(String)` on failure. It is surfaced as `FederationError::Delivery`.
**Example:**
```rust
// Implement LocalDelivery to receive opened inbound envelopes.
// Return Err(String) on failure.
```

### `Peer`
**Signature:** `pub use crate::federation::Peer;`
**Purpose:** A registered remote node. It holds the namespace, the endpoint, the shared key, and the provenance stamped on everything received from it.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// A Peer holds a namespace, an endpoint, a PeerKey, and the provenance
// stamped on everything received from it.
```

### `PeerDirectory`
**Signature:** `pub use crate::federation::PeerDirectory;`
**Purpose:** Known peers, by namespace. `is_remote(ns)` decides between local and federated routing.
**Parameters:** none
**Returns:** nothing. `is_remote(ns)` reports whether the namespace is remote.
**Errors:** none
**Example:**
```rust
if directory.is_remote("billing") {
    // route to a federated node
}
```

### `PeerKey`
**Signature:** `pub use crate::federation::PeerKey;`
**Purpose:** A 32-byte pre-shared symmetric key for one peer link.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
// `key` is a PeerKey: 32 bytes, shared with one peer.
let frame = seal(&envelope, "node-a", &key)?;
```

### `Authorizer`
**Signature:** `pub use crate::federation::Authorizer;`
**Purpose:** Host policy at the federation seam. `authorize(from, to)` decides whether the edge-stamped (unforgeable) origin `from` may reach target `to`. It is called after stamping and before local delivery. Rejection fails closed.
**Parameters:**
- `authorize(from, ..)`: the authenticated origin address.
- `authorize(.., to)`: the target address.

**Returns:** Not stated in the doc comment.
**Errors:** Return `Err(reason)` to refuse. It is surfaced as `FederationError::Unauthorized`.
**Example:**
```rust
// Implement Authorizer::authorize(from, to).
// Return Err(reason) to refuse.
```

### `AllowAll`
**Signature:** `pub use crate::federation::AllowAll;`
**Purpose:** An `Authorizer` that admits every authenticated peer. Intended for tests and trusted setups.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
let server = FederationServer::new(node, directory, transport, local, AllowAll);
```

### `FederationError`
**Signature:** `pub use crate::federation::FederationError;`
**Purpose:** Everything that can go wrong at the federation boundary. Variants: `UnknownPeer(String)`, `MalformedFrame`, `UnsupportedVersion(u8)`, `UnsupportedAuth(u8)`, `OpenFailed`, `Nonce`, `NoDestination`, `NotRemote(String)`, `Transport(String)`, `Delivery(String)`, `Unauthorized(String)`, and `Codec(PipelineError)`.
**Parameters:** none
**Returns:** nothing
**Errors:** none
**Example:**
```rust
match server.receive(frame).await {
    Err(FederationError::Unauthorized(reason)) => { /* refused by the Authorizer */ }
    Err(FederationError::UnknownPeer(name)) => { /* not in the peer directory */ }
    _ => {}
}
```

### `seal`
**Signature:** `pub fn seal(envelope: &Envelope, sender: &str, key: &PeerKey) -> Result<Vec<u8>, FederationError>`
**Purpose:** Seals an envelope for a peer. It encodes the envelope and encrypts it with the peer's key.
**Parameters:**
- `envelope`: the envelope to send.
- `sender`: this node's namespace, which the key authenticates.
- `key`: the peer link's shared key.

**Returns:** The sealed frame bytes.
**Errors:** `FederationError::Codec` when encoding fails. `FederationError::Nonce` when no nonce can be generated. `FederationError::OpenFailed` when encryption fails.
**Example:**
```rust
let frame = seal(&envelope, "node-a", &key)?;
```

### `open`
**Signature:** `pub fn open(frame: &[u8], directory: &PeerDirectory) -> Result<(Envelope, String), FederationError>`
**Purpose:** Opens a sealed frame and authenticates the sender against the peer directory.
**Parameters:**
- `frame`: the received bytes.
- `directory`: the known peers and their keys.

**Returns:** The envelope and the authenticated sender namespace. The envelope is still untrusted.
**Errors:** `FederationError::UnknownPeer`, `MalformedFrame`, `UnsupportedVersion`, `UnsupportedAuth`, `OpenFailed` (wrong key or tampering), or `Codec`.
**Example:**
```rust
let (envelope, sender) = open(&frame, &directory)?;
// `envelope` is still untrusted.
```

## Changes since the last version
v1.0.1 (docs only). No code, signatures or behaviour changed.

- **Added:** none
- **Changed:** doc comments only, on `Pipeline`, `CallKind`, `ThreadFailer`, `Switchboard`, `SwitchboardError`, `FederationServer` and `FederationError`
- **Removed:** none

## Not part of the API
Everything outside the wrapper module `rust_pipeline::api` stays inside the project. Callers should not reach past it. The internal modules `codec`, `wire` and `pipeline` are still `pub` for existing callers, but they are `#[doc(hidden)]` and may change without notice.
