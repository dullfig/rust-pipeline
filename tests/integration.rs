//! Integration tests for rust-pipeline.
//!
//! These tests verify the full message flow:
//! inject raw bytes → parse → validate → route → dispatch → reinject → ...
//!
//! Two handlers: echo (replies with same payload) and uppercase (transforms text).
//! Messages chain through them to verify the self-feeding pipeline works end-to-end.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use rust_pipeline::prelude::*;

// ── Test Handlers ────────────────────────────────────────────────────

/// Forward handler: receives a Greeting and forwards to "sink".
struct ForwardHandler;

#[async_trait]
impl Handler for ForwardHandler {
    async fn handle(&self, payload: ValidatedPayload, _ctx: HandlerContext) -> HandlerResult {
        // Transform the payload tag (simulating a type conversion)
        let xml_str = String::from_utf8_lossy(&payload.xml);
        let transformed = xml_str.replace("Greeting", "Response");
        Ok(HandlerResponse::Send {
            to: "sink".into(),
            payload_xml: transformed.into_bytes(),
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
        let text = String::from_utf8_lossy(&payload.xml).to_string();
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

    let envelope = build_envelope(
        "external",
        "sink",
        "thread-001",
        b"<Greeting><text>hello world</text></Greeting>",
    )
    .unwrap();

    pipeline.inject(envelope).await.unwrap();

    // Wait for processing
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

    // Forwarder: receives Greeting, sends Response to sink
    registry.register(
        "forwarder",
        "Greeting",
        ForwardHandler,
        false,
        vec![],
        "Forwards greeting as response",
        None,
    );

    // Sink: receives Response, records it
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

    let envelope = build_envelope(
        "test-sender",
        "forwarder",
        "thread-chain-001",
        b"<Greeting><text>chain test</text></Greeting>",
    )
    .unwrap();

    pipeline.inject(envelope).await.unwrap();

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

    // Sink (the unauthorized target)
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

    let envelope = build_envelope(
        "system",
        "restricted-agent",
        "thread-peer-001",
        b"<Greeting><text>sneaky message</text></Greeting>",
    )
    .unwrap();

    pipeline.inject(envelope).await.unwrap();

    // Wait for processing
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
        let payload = format!("<Greeting><text>msg-{i}</text></Greeting>");
        let thread = format!("thread-{i:03}");
        let envelope = build_envelope("sender", "sink", &thread, payload.as_bytes()).unwrap();
        pipeline.inject(envelope).await.unwrap();
    }

    // Wait for all to process
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let msgs = received.lock().await;
    assert_eq!(msgs.len(), 10, "all 10 messages should be received");

    pipeline.shutdown().await;
}

#[tokio::test]
async fn malformed_xml_rejected() {
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

    // Inject garbage bytes — should be rejected at parse stage
    pipeline
        .inject(b"this is not XML at all".to_vec())
        .await
        .unwrap();

    // Inject truncated XML
    pipeline
        .inject(b"<message><meta><from>x</from>".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msgs = received.lock().await;
    assert_eq!(
        msgs.len(),
        0,
        "malformed messages should be rejected at parse stage"
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
    let original_payload = b"<TestPayload><value>42</value><nested><deep>data</deep></nested></TestPayload>";

    let bytes = build_envelope("alice", "bob", "thread-rt", original_payload).unwrap();

    // Parse it back
    let env = parse_envelope(&bytes).unwrap();
    assert_eq!(env.meta.from, "alice");
    assert_eq!(env.meta.to.as_deref(), Some("bob"));
    assert_eq!(env.meta.thread, "thread-rt");
    assert_eq!(env.payload_tag, "TestPayload");

    let payload_str = String::from_utf8_lossy(&env.payload_raw);
    assert!(payload_str.contains("42"));
    assert!(payload_str.contains("deep"));
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
