//! rust-pipeline — Self-feeding async pipeline with adversarial validation.
//!
//! Spiritual successor to xml-pipeline. Keeps the core pattern —
//! self-feeding async pipeline with adversarial validation — but
//! rethinks it for Rust idioms.
//!
//! # Zero Trust
//!
//! No agent response is trusted. Ever. Every message, including handler
//! responses that get re-injected, enters as raw untrusted bytes and goes
//! through the full validation gauntlet. The pipeline IS the trust boundary.
//!
//! ```text
//! [Raw Bytes] → parse → validate(XSD) → route → enforce(peers) → dispatch(handler)
//!      ↑                                                              |
//!      └────── serialize to raw bytes (UNTRUSTED) ────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use rust_pipeline::prelude::*;
//!
//! # async fn example() {
//! // 1. Create registry and register handlers
//! let mut registry = ListenerRegistry::new();
//! let threads = ThreadRegistry::new();
//!
//! registry.register(
//!     "echo",
//!     "Greeting",
//!     FnHandler(|p: ValidatedPayload, _ctx: HandlerContext| {
//!         Box::pin(async move {
//!             Ok(HandlerResponse::Reply { payload_xml: p.xml })
//!         })
//!     }),
//!     false,
//!     vec![],
//!     "Echo handler",
//!     None,
//! );
//!
//! // 2. Create and start pipeline
//! let mut pipeline = Pipeline::new(registry, threads);
//! pipeline.run();
//!
//! // 3. Inject messages
//! let envelope = build_envelope("sender", "echo", "thread-1", b"<Greeting><text>hi</text></Greeting>").unwrap();
//! pipeline.inject(envelope).await.unwrap();
//!
//! // 4. Shutdown when done
//! pipeline.shutdown().await;
//! # }
//! ```

pub mod config;
pub mod envelope;
pub mod error;
pub mod handler;
pub mod middleware;
pub mod pipeline;
pub mod registry;
pub mod routing;
pub mod thread;
pub mod validation;

/// Convenient re-exports for common usage.
pub mod prelude {
    pub use crate::config::{load_config, parse_config, ListenerConfig, PipelineConfig};
    pub use crate::envelope::{build_envelope, parse_envelope, AgentId, Envelope, Meta, ThreadId};
    pub use crate::error::{PipelineError, PipelineResult};
    pub use crate::handler::{
        FnHandler, Handler, HandlerContext, HandlerResponse, HandlerResult, ValidatedPayload,
    };
    pub use crate::middleware::{
        DispatchMeta, Middleware, PostDispatchVerdict, PreDispatchVerdict,
    };
    pub use crate::pipeline::Pipeline;
    pub use crate::registry::ListenerRegistry;
    pub use crate::routing::RoutingTable;
    pub use crate::thread::ThreadRegistry;
    pub use crate::validation::{
        FieldSchema, FieldType, PayloadSchema, SchemaRegistry,
    };
}
