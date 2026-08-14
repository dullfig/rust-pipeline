//! Handler trait and associated types.
//!
//! Handlers receive **validated, typed** payloads — the pipeline has already decoded
//! the wire bytes and schema-checked the value by the time a handler sees the message.
//! Handlers never touch wire bytes (the encapsulation rider): they work with
//! [`PayloadValue`] in and a typed [`Payload`] out. rust-pipeline owns serialization on
//! both sides, so the wire format (XML now, binary later) is invisible here.
//!
//! Handler *responses* are NOT trusted: they are serialized back to bytes and re-enter
//! the pipeline from the top. The trust boundary is the pipeline, not the handler.

use async_trait::async_trait;

use crate::envelope::ThreadId;
use crate::wire::{Address, Payload, PayloadValue, Provenance};

/// Validated payload that handlers receive.
///
/// By the time a handler gets this, the pipeline has decoded the envelope, schema-checked
/// the value, and verified routing. `value` is a typed [`PayloadValue`] — never raw bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedPayload {
    /// The payload tag (e.g., "Greeting") — selects the operation/schema.
    pub tag: String,
    /// The decoded, schema-validated value.
    pub value: PayloadValue,
}

impl ValidatedPayload {
    pub fn new(tag: impl Into<String>, value: PayloadValue) -> ValidatedPayload {
        ValidatedPayload {
            tag: tag.into(),
            value,
        }
    }

    /// Clone this into an outgoing [`Payload`] (e.g., for an echo handler).
    pub fn to_payload(&self) -> Payload {
        Payload {
            tag: self.tag.clone(),
            value: self.value.clone(),
        }
    }
}

/// Context passed to handlers alongside the payload.
///
/// Provides verified metadata without giving handlers the ability to forge identity or
/// bypass routing. `from` is a full [`Address`] (last hop) — set by the pipeline.
#[derive(Debug, Clone)]
pub struct HandlerContext {
    /// Thread UUID for this conversation chain.
    pub thread_id: ThreadId,
    /// Who sent this message (verified by pipeline, not forgeable by handler).
    pub from: Address,
    /// This handler's own address (set by pipeline, not self-reported).
    pub own_name: Address,
    /// The message's accumulated provenance set, set by the pipeline.
    ///
    /// **Observation only** (the watcher posture). A handler MUST NOT gate on this —
    /// egress enforcement is a separate mechanical wall (agentos policy). It's exposed
    /// read-only so observers/audit can see the durable origin set; the handler cannot
    /// forge it (the pipeline owns the field).
    pub provenance: Provenance,
}

/// What a handler can return.
///
/// Handlers express *intent* — the pipeline enforces the rules. A handler can't forge
/// identity (`from` is always overwritten by the pipeline) and can't escape peers (the
/// pipeline validates `to` against the peer table). Outgoing payloads are typed; the
/// pipeline serializes them — handlers never produce bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum HandlerResponse {
    /// Send a new message to a target address and **wait** for its reply (a synchronous
    /// sub-call). The pipeline enforces peer constraints, extends the thread chain, and
    /// suspends this conversation until the callee replies back up the chain.
    Send { to: Address, payload: Payload },

    /// **Asynchronously** delegate to a target and continue immediately — the async dual of
    /// [`Send`](HandlerResponse::Send).
    ///
    /// The pipeline hands the work to `to` on a detached branch whose chain records *this*
    /// handler as the return target, then synthesizes an immediate acknowledgement (a
    /// `SpawnAck` payload) back to this handler so it can proceed — e.g. tell its own caller
    /// "working on it" — without blocking on the result. The eventual result routes back
    /// through the ordinary reply path, so **the callee never learns who spawned it**;
    /// return routing is the pipeline's bookkeeping (the thread chain), not the callee's
    /// concern.
    ///
    /// This is "received ≠ done": the ack means *accepted for processing*, and the pipeline
    /// only acks once the handoff is routable (peer-checked) — a spawn that can't be
    /// delivered acks with `accepted: false` rather than a false "working on it".
    Spawn { to: Address, payload: Payload },

    /// Respond back to the caller (prune the thread chain).
    Reply { payload: Payload },

    /// No response — this handler has nothing to say.
    /// If a parent exists in the thread chain, the pipeline synthesizes an ACK
    /// (`ToolResponse { success: true, result: "ack" }`) and routes it back so the parent
    /// doesn't hang. If no parent exists, the thread ends silently.
    None,
}

impl HandlerResponse {
    /// Convenience: reply with a payload.
    pub fn reply(payload: Payload) -> HandlerResponse {
        HandlerResponse::Reply { payload }
    }

    /// Convenience: send a payload to a target.
    pub fn send(to: impl Into<Address>, payload: Payload) -> HandlerResponse {
        HandlerResponse::Send {
            to: to.into(),
            payload,
        }
    }

    /// Convenience: asynchronously delegate a payload to a target.
    pub fn spawn(to: impl Into<Address>, payload: Payload) -> HandlerResponse {
        HandlerResponse::Spawn {
            to: to.into(),
            payload,
        }
    }
}

/// The result type handlers return.
pub type HandlerResult = Result<HandlerResponse, crate::error::PipelineError>;

/// The core handler trait.
///
/// # Trust Model
///
/// - Handlers receive **validated, typed** payloads (trust earned by pipeline)
/// - Handler responses are **untrusted** (serialized back to bytes, re-validated)
/// - The pipeline enforces identity, routing, and peer constraints
/// - Handlers express intent; the pipeline enforces policy
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Process a validated payload and optionally produce a response.
    async fn handle(&self, payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult;

    /// Serialize this instance's state surface (gist, state-of-mind, KV memory, …).
    ///
    /// `None` (the default) means a **stateless** handler — nothing to persist — so every
    /// existing handler keeps compiling untouched. A stateful handler overrides this to hand
    /// the platform an **opaque** blob: rust-pipeline (and the host above it) moves and
    /// persists the bytes but never interprets them.
    ///
    /// # Containment
    ///
    /// This is a *separate channel* from [`Handler::handle`] — a checkpoint blob is NOT a
    /// [`HandlerResponse`], is never serialized into an envelope, and never re-enters the
    /// pipeline. The zero-trust re-entry gate (PROTOCOL §2.2) is therefore untouched: a
    /// handler's state surface can only influence the world through a `handle` response,
    /// which is stamped with the thread's accumulated provenance at the egress seam
    /// (PROTOCOL §16). Treat the blob as **sensitive at rest** — it may hold tainted content,
    /// same trust class as the context store — and as strictly **per-instance** (see
    /// [`Handler::restore`]).
    async fn checkpoint(&self) -> Option<Vec<u8>> {
        None
    }

    /// Rehydrate from a blob this **same instance** previously produced via
    /// [`Handler::checkpoint`].
    ///
    /// Called at materialize time, before the instance is published to the switchboard, so
    /// `&mut self` is clean — the instance is not yet shared and no `handle` call can be in
    /// flight. The default is a no-op (stateless handlers have nothing to restore).
    ///
    /// The blob handed in is only ever *this* instance's own prior state — never another
    /// instance's, never another tenant's. `restore(checkpoint())` is expected to round-trip
    /// faithfully; lossy consolidation (folding state into a durable synopsis) is a separate,
    /// explicit host step and must never be smuggled in here.
    async fn restore(&mut self, _blob: &[u8]) {}
}

/// Convenience: wrap a closure as a handler.
pub struct FnHandler<F>(pub F);

#[async_trait]
impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(ValidatedPayload, HandlerContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = HandlerResult> + Send,
{
    async fn handle(&self, payload: ValidatedPayload, ctx: HandlerContext) -> HandlerResult {
        (self.0)(payload, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fn_handler_works() {
        let handler = FnHandler(|payload: ValidatedPayload, _ctx: HandlerContext| async move {
            // Echo: reply with the same payload
            Ok(HandlerResponse::Reply {
                payload: payload.to_payload(),
            })
        });

        let payload = ValidatedPayload::new(
            "Greeting",
            PayloadValue::record([("text", PayloadValue::text("hi"))]),
        );
        let ctx = HandlerContext {
            thread_id: "t1".into(),
            from: "alice".into(),
            own_name: "echo".into(),
            provenance: Provenance::EMPTY,
        };

        let result = handler.handle(payload, ctx).await.unwrap();
        match result {
            HandlerResponse::Reply { payload } => {
                assert_eq!(payload.tag, "Greeting");
                assert_eq!(
                    payload.value.get("text").and_then(|v| v.as_text()),
                    Some("hi")
                );
            }
            _ => panic!("expected Reply"),
        }
    }

    /// A stateless handler (and `FnHandler`) inherits the default state surface:
    /// `checkpoint` yields `None`, so the OS persists nothing for it.
    #[tokio::test]
    async fn stateless_handler_has_no_state_surface() {
        let handler = FnHandler(|_p: ValidatedPayload, _c: HandlerContext| async move {
            Ok(HandlerResponse::None)
        });
        assert_eq!(handler.checkpoint().await, None);
    }

    /// A stateful handler overrides the surface; `restore(checkpoint())` round-trips
    /// (INV-FIDELITY at the trait-plumbing level — the blob stays opaque to the platform).
    #[tokio::test]
    async fn stateful_handler_checkpoint_restore_roundtrips() {
        struct Counter {
            count: u32,
        }

        #[async_trait]
        impl Handler for Counter {
            async fn handle(
                &self,
                _payload: ValidatedPayload,
                _ctx: HandlerContext,
            ) -> HandlerResult {
                Ok(HandlerResponse::None)
            }

            async fn checkpoint(&self) -> Option<Vec<u8>> {
                Some(self.count.to_le_bytes().to_vec())
            }

            async fn restore(&mut self, blob: &[u8]) {
                self.count = u32::from_le_bytes(blob.try_into().expect("4-byte blob"));
            }
        }

        let original = Counter { count: 42 };
        let blob = original.checkpoint().await.expect("stateful → Some");

        let mut revived = Counter { count: 0 };
        revived.restore(&blob).await;

        assert_eq!(revived.count, 42);
    }
}
