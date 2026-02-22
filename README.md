# rust-pipeline

A self-feeding async pipeline with adversarial validation.

Messages enter as raw bytes, pass through parsing, schema validation, routing,
and dispatch. Handler responses are serialized back to raw bytes and re-injected
at the ingress — losing all trust, going through the full gauntlet again.

The pipeline is the trust boundary. Not the handler.

```
[Raw Bytes] ──→ Parse ──→ Validate ──→ Route ──→ Dispatch ──→ Handler
     ↑                                                           │
     └──────────── serialize to raw bytes (UNTRUSTED) ───────────┘
```

## Why

Most agent frameworks trust their own output. A handler returns a response,
and the framework delivers it without question. That works until an LLM
hallucinates a message to a target it shouldn't reach, or a tool response
contains a payload that would fail validation if anyone bothered to check.

rust-pipeline bothers to check. Every time. Including its own output.

## Core Concepts

**Handlers** receive `ValidatedPayload` — by the time they see a message,
the pipeline has parsed the envelope, validated against schema, and verified
routing. But handler responses are *not* trusted. They re-enter as raw bytes.

```rust
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult;
}
```

**HandlerResponse** expresses intent, not action. The pipeline enforces the rules:

```rust
pub enum HandlerResponse {
    Reply { payload_xml: Vec<u8> },     // respond to caller
    Send { to: AgentId, payload_xml: Vec<u8> },  // forward to peer
    None,                                // nothing to say
}
```

A handler cannot forge identity — `from` is always overwritten by the pipeline.
A handler cannot escape its peers — `to` is validated against the peer table.
A handler cannot skip validation — responses re-enter as untrusted bytes.

**Threads** track conversation chains. Every message lives on a thread.
Threads can branch (A sends to B sends to C) and recurse (`root.a.b.c.c.c...`),
making the pipeline Turing-complete: conditional branching + arbitrary memory +
unbounded recursion.

**Peers** constrain who can talk to whom. A listener declares its peers at
registration. The pipeline enforces this structurally — if a handler tries
to send to a non-peer, the message is rejected. No runtime check. No flag.
The route simply does not exist.

## Quick Start

```rust
use rust_pipeline::prelude::*;

#[tokio::main]
async fn main() {
    let mut registry = ListenerRegistry::new();
    let threads = ThreadRegistry::new();

    // Register a handler that echoes back what it receives
    registry.register(
        "echo",
        "Greeting",
        FnHandler(|p: ValidatedPayload, _ctx: HandlerContext| {
            Box::pin(async move {
                Ok(HandlerResponse::Reply { payload_xml: p.xml })
            })
        }),
        false,
        vec![],
        "Echo handler",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    let envelope = build_envelope(
        "sender", "echo", "thread-1",
        b"<Greeting><text>hello</text></Greeting>"
    ).unwrap();
    pipeline.inject(envelope).await.unwrap();

    pipeline.shutdown().await;
}
```

## Modules

| Module | Purpose |
|--------|---------|
| `pipeline` | The self-feeding loop: ingress, parse, validate, route, dispatch, re-inject |
| `handler` | `Handler` trait, `ValidatedPayload`, `HandlerResponse`, `HandlerContext` |
| `registry` | Listener registration: name, schema tag, handler, peers, description |
| `envelope` | XML envelope: `from`, `to`, `thread_id`, `payload` — the wire format |
| `routing` | Routing table: peer enforcement, target resolution |
| `thread` | Thread registry: conversation chains, branching, recursion |
| `validation` | Schema registry: field-level validation before dispatch |
| `config` | YAML-driven pipeline configuration |
| `error` | Error types: `PipelineError`, `PipelineResult` |

## Design Principles

- **Zero trust.** Handler responses re-enter as untrusted bytes. Always.
- **Identity is not self-reported.** The pipeline sets `from`. Handlers cannot forge it.
- **Routing is structural.** Missing peer = impossible route. Not a runtime error — a compile-time impossibility.
- **Backpressure is natural.** Stages are connected by bounded tokio channels. A slow handler slows the pipeline. No message is dropped.
- **The pipeline is the only authority.** Handlers express intent. The pipeline decides.

## Numbers

- **~2,500** lines of Rust
- **42** tests
- **8** modules
- **0** unsafe blocks

## Used By

- [BestCode](https://github.com/dullfig/BestCode) — the AgentOS kernel, built on this pipeline

## License

MIT
