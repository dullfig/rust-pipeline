# rust-pipeline

Self-feeding async pipeline with adversarial validation. Spiritual successor to xml-pipeline (Python), redesigned for Rust.

## Architecture

Zero trust: every message enters as raw `Vec<u8>` and earns trust through pipeline stages. Handler responses are serialized back to bytes and re-injected as untrusted — the pipeline IS the trust boundary.

```
[Raw Bytes] → parse → validate(schema) → route → enforce(peers) → dispatch(handler)
     ↑                                                                  |
     └────── serialize to raw bytes (UNTRUSTED) ────────────────────────┘
```

4 tokio tasks connected by bounded mpsc channels. Each stage is independent.

## Module Map

- `error.rs` — PipelineError enum (thiserror)
- `envelope.rs` — XML envelope parse/build (quick-xml). Namespace: `https://xml-pipeline.org/ns/envelope/v1`
- `handler.rs` — Handler trait, FnHandler, HandlerResponse {Send, Reply, None}
- `thread.rs` — ThreadRegistry: UUID↔chain bidirectional map
- `routing.rs` — RoutingTable + peer enforcement
- `validation.rs` — Structural schema validation (PayloadSchema, SchemaRegistry)
- `registry.rs` — ListenerRegistry (handlers + routing + schemas)
- `pipeline.rs` — The self-feeding pipeline (parse → validate → route → dispatch stages)
- `config.rs` — YAML config (serde_yaml), same format as xml-pipeline's organism.yaml
- `lib.rs` — Public API + prelude

## Testing

`cargo test` — 51 tests (42 unit + 8 integration + 1 doc test).

## Conventions

- Envelope XML format matches xml-pipeline for compatibility
- Routing key: `listener_name.payload_tag` (lowercase)
- Thread chains: dot-separated (e.g., `console.router.greeter`), UUIDs obscure topology
- Handlers receive validated payloads; responses are NEVER trusted
