//! Integration tests for rust-pipeline.
//!
//! These tests verify the full message flow:
//! inject raw bytes → decode → validate → route → dispatch → reinject → ...
//!
//! Handlers work with typed values (the encapsulation rider) — never wire bytes.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use rust_pipeline::prelude::*;

// ── Helpers ──────────────────────────────────────────────────────────

/// Build inbound wire bytes for a message.
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

// ── Test Handlers ────────────────────────────────────────────────────

/// Forward handler: receives a Greeting and forwards it (re-tagged) to "sink".
struct ForwardHandler;

#[async_trait]
impl Handler for ForwardHandler {
    async fn handle(&self, payload: ValidatedPayload, _ctx: HandlerContext) -> HandlerResult {
        // Re-tag the payload (Greeting → Response), preserving its value.
        Ok(HandlerResponse::Send {
            to: "sink".into(),
            payload: Payload::new("Response", payload.value.clone()),
        })
    }
}

/// Sink handler: records received messages for verification.
struct SinkHandler {
    received: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Handler for SinkHandler {
    async fn handle(&self, payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult {
        let text = payload
            .value
            .get("text")
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string();
        self.received.lock().await.push(format!(
            "from={} thread={} payload={}",
            ctx.from, ctx.thread_id, text
        ));
        Ok(HandlerResponse::None)
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_handler_receives_message() {
    let received = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut registry = ListenerRegistry::new();
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test");

    registry.register(
        "sink",
        "Greeting",
        SinkHandler {
            received: received.clone(),
        },
        false,
        vec![],
        "Sink handler",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    pipeline
        .inject(inbound(
            "external",
            "sink",
            "thread-001",
            Payload::single("Greeting", "text", "hello world"),
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msgs = received.lock().await;
    assert_eq!(msgs.len(), 1, "sink should receive exactly one message");
    assert!(msgs[0].contains("hello world"), "payload should contain text");
    assert!(msgs[0].contains("from=external"), "should know the sender");

    pipeline.shutdown().await;
}

#[tokio::test]
async fn two_handler_chain() {
    // forwarder → sink chain
    let received = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut registry = ListenerRegistry::new();
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test");

    registry.register(
        "forwarder",
        "Greeting",
        ForwardHandler,
        false,
        vec![],
        "Forwards greeting as response",
        None,
    );

    registry.register(
        "sink",
        "Response",
        SinkHandler {
            received: received.clone(),
        },
        false,
        vec![],
        "Records messages",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    pipeline
        .inject(inbound(
            "test-sender",
            "forwarder",
            "thread-chain-001",
            Payload::single("Greeting", "text", "chain test"),
        ))
        .await
        .unwrap();

    // Wait for two hops: forwarder → reinject → sink
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let msgs = received.lock().await;
    assert_eq!(msgs.len(), 1, "sink should receive the forwarded message");
    assert!(
        msgs[0].contains("chain test"),
        "payload should survive the chain"
    );
    assert!(
        msgs[0].contains("from=forwarder"),
        "sender should be forwarder (not original sender)"
    );

    pipeline.shutdown().await;
}

#[tokio::test]
async fn peer_enforcement_blocks_unauthorized() {
    let received = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut registry = ListenerRegistry::new();
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test");

    // Agent with peers: can only talk to "allowed-target"
    registry.register(
        "restricted-agent",
        "Greeting",
        ForwardHandler, // tries to send to "sink"
        true,           // is_agent = true
        vec!["allowed-target".into()], // NOT "sink"
        "Restricted agent",
        None,
    );

    registry.register(
        "sink",
        "Response",
        SinkHandler {
            received: received.clone(),
        },
        false,
        vec![],
        "Should NOT receive",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    pipeline
        .inject(inbound(
            "system",
            "restricted-agent",
            "thread-peer-001",
            Payload::single("Greeting", "text", "sneaky message"),
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let msgs = received.lock().await;
    assert_eq!(
        msgs.len(),
        0,
        "sink should NOT receive message (peer violation)"
    );

    pipeline.shutdown().await;
}

#[tokio::test]
async fn multiple_messages_processed() {
    let received = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut registry = ListenerRegistry::new();
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test");

    registry.register(
        "sink",
        "Greeting",
        SinkHandler {
            received: received.clone(),
        },
        false,
        vec![],
        "Collects all messages",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    // Inject 10 messages
    for i in 0..10 {
        let thread = format!("thread-{i:03}");
        pipeline
            .inject(inbound(
                "sender",
                "sink",
                &thread,
                Payload::single("Greeting", "text", format!("msg-{i}")),
            ))
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let msgs = received.lock().await;
    assert_eq!(msgs.len(), 10, "all 10 messages should be received");

    pipeline.shutdown().await;
}

#[tokio::test]
async fn malformed_bytes_rejected() {
    let received = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut registry = ListenerRegistry::new();
    let threads = ThreadRegistry::new();

    registry.register(
        "sink",
        "Greeting",
        SinkHandler {
            received: received.clone(),
        },
        false,
        vec![],
        "Should NOT receive",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    // Garbage bytes — rejected at decode stage
    pipeline
        .inject(b"this is not XML at all".to_vec())
        .await
        .unwrap();

    // Truncated envelope — rejected at decode stage
    pipeline
        .inject(b"<message><meta><from>x</from>".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msgs = received.lock().await;
    assert_eq!(
        msgs.len(),
        0,
        "malformed messages should be rejected at decode stage"
    );

    pipeline.shutdown().await;
}

#[tokio::test]
async fn config_roundtrip() {
    let yaml = r#"
organism:
  name: integration-test
  port: 9999

max_concurrent_handlers: 10

listeners:
  - name: greeter
    payload_class: handlers.Greeting
    handler: handlers.handle_greeting
    description: Test greeter
    agent: true
    peers: [responder]

  - name: responder
    payload_class: handlers.Response
    handler: handlers.handle_response
    description: Test responder
"#;

    let config = parse_config(yaml).unwrap();
    assert_eq!(config.organism.name, "integration-test");
    assert_eq!(config.organism.port, 9999);
    assert_eq!(config.max_concurrent_handlers, 10);
    assert_eq!(config.listeners.len(), 2);
    assert_eq!(config.listeners[0].payload_tag(), "Greeting");
    assert!(config.listeners[0].agent);
    assert_eq!(config.listeners[0].peers, vec!["responder"]);
}

#[tokio::test]
async fn envelope_roundtrip_preserves_data() {
    let env = Envelope {
        meta: Meta {
            from: "alice".into(),
            to: Some("bob".into()),
            thread: "thread-rt".into(),
            provenance: Provenance::from_bit(2),
        },
        payload: Payload::new(
            "TestPayload",
            PayloadValue::record([
                ("value", PayloadValue::Uint(42)),
                (
                    "nested",
                    PayloadValue::record([("deep", PayloadValue::text("data"))]),
                ),
            ]),
        ),
    };

    let bytes = encode_envelope(&env).unwrap();
    let back = decode_envelope(&bytes).unwrap();

    // Full structural round-trip, including provenance.
    assert_eq!(back, env);
    assert!(back.meta.provenance.contains_bit(2));
    assert_eq!(back.meta.from.target(), Some("alice"));
    assert_eq!(back.payload.value.get("value"), Some(&PayloadValue::Uint(42)));
}

#[tokio::test]
async fn thread_chain_lifecycle() {
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test-org");

    // Simulate: console → router → greeter → shouter
    let t1 = threads.start_chain("console", "router");
    let t2 = threads.extend_chain(&t1, "greeter");
    let t3 = threads.extend_chain(&t2, "shouter");

    assert_eq!(threads.lookup(&t3), Some("console.router.greeter.shouter"));

    // Shouter responds → prune to greeter
    let prune1 = threads.prune_for_response(&t3).unwrap();
    assert_eq!(prune1.target, "greeter");

    // Greeter responds → prune to router
    let prune2 = threads.prune_for_response(&prune1.thread_id).unwrap();
    assert_eq!(prune2.target, "router");

    // Router responds → prune to console
    let prune3 = threads.prune_for_response(&prune2.thread_id).unwrap();
    assert_eq!(prune3.target, "console");

    // Console responds → chain exhausted
    assert!(threads.prune_for_response(&prune3.thread_id).is_none());
}

/// Records the provenance set observed by the handler (the watcher posture).
struct ProvSink {
    seen: Arc<Mutex<Option<Provenance>>>,
}

#[async_trait]
impl Handler for ProvSink {
    async fn handle(&self, _payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult {
        *self.seen.lock().await = Some(ctx.provenance);
        Ok(HandlerResponse::None)
    }
}

#[tokio::test]
async fn provenance_propagates_across_hops() {
    // Inject with a two-bit provenance set (one bit crosses the 64-bit boundary),
    // route through forwarder → sink, and assert the FULL set survives the hop —
    // i.e. the dispatcher unions inbound provenance into the envelope it rebuilds.
    let seen = Arc::new(Mutex::new(None));

    let mut registry = ListenerRegistry::new();
    let mut threads = ThreadRegistry::new();
    threads.initialize_root("test");

    registry.register(
        "forwarder",
        "Greeting",
        ForwardHandler,
        false,
        vec![],
        "Forwards (carries provenance)",
        None,
    );
    registry.register(
        "sink",
        "Response",
        ProvSink { seen: seen.clone() },
        false,
        vec![],
        "Observes provenance",
        None,
    );

    let mut pipeline = Pipeline::new(registry, threads);
    pipeline.run();

    let env = Envelope {
        meta: Meta {
            from: "ext".into(),
            to: Some("forwarder".into()),
            thread: "thread-prov".into(),
            provenance: Provenance::from_bit(3).union(Provenance::from_bit(70)),
        },
        payload: Payload::single("Greeting", "text", "x"),
    };
    pipeline.inject(encode_envelope(&env).unwrap()).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let observed = seen.lock().await.expect("sink should have observed provenance");
    assert!(observed.contains_bit(3), "bit 3 should survive the hop");
    assert!(observed.contains_bit(70), "bit 70 (high word) should survive the hop");
    assert!(!observed.contains_bit(5), "unset bit must not appear");

    pipeline.shutdown().await;
}
