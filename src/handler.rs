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
    /// Send a new message to a target address (forward).
    /// Pipeline enforces peer constraints and extends the thread chain.
    Send { to: Address, payload: Payload },

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
}
