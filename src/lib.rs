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
//! [Raw Bytes] → decode → validate(schema) → route → enforce(peers) → dispatch(handler)
//!      ↑                                                                  |
//!      └────── encode to raw bytes (UNTRUSTED) ────────────────────────────┘
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
//!             Ok(HandlerResponse::Reply { payload: p.to_payload() })
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
//! // 3. Inject messages — handlers work with typed values, never bytes
//! let envelope = Envelope {
//!     meta: Meta {
//!         from: "sender".into(),
//!         to: Some("echo".into()),
//!         thread: "thread-1".into(),
//!         provenance: Provenance::EMPTY,
//!     },
//!     payload: Payload::single("Greeting", "text", "hi"),
//! };
//! pipeline.inject(encode_envelope(&envelope).unwrap()).await.unwrap();
//!
//! // 4. Shutdown when done
//! pipeline.shutdown().await;
//! # }
//! ```

pub mod api;

// Internal modules. Still `pub` so existing callers keep compiling while they migrate to
// `api`, but hidden from docs and NOT part of the contract — import from `api` instead.
#[doc(hidden)]
pub mod codec;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod envelope;
#[doc(hidden)]
pub mod federation;
#[doc(hidden)]
pub mod error;
#[doc(hidden)]
pub mod handler;
#[doc(hidden)]
pub mod middleware;
#[doc(hidden)]
pub mod pipeline;
#[doc(hidden)]
pub mod registry;
#[doc(hidden)]
pub mod routing;
#[doc(hidden)]
pub mod switchboard;
#[doc(hidden)]
pub mod thread;
#[doc(hidden)]
pub mod validation;
#[doc(hidden)]
pub mod wire;

/// Glob-import convenience: exactly the [`api`] surface.
pub mod prelude {
    pub use crate::api::*;
}
