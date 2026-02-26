//! Handler trait and associated types.
//!
//! Handlers receive validated payloads — the pipeline has already done the
//! XSD check by the time a handler sees the message. But handler *responses*
//! are NOT trusted. They get serialized back to raw bytes and re-enter the
//! pipeline from the top. The trust boundary is the pipeline, not the handler.

use async_trait::async_trait;

use crate::envelope::{AgentId, ThreadId};

/// Validated payload that handlers receive.
///
/// By the time a handler gets this, the pipeline has:
/// 1. Parsed the envelope (well-formed XML)
/// 2. Validated against XSD schema
/// 3. Verified routing (target exists, peers enforced)
///
/// The payload is raw XML bytes of the validated payload element.
#[derive(Debug, Clone)]
pub struct ValidatedPayload {
    /// Raw XML bytes of the payload element (already validated).
    pub xml: Vec<u8>,
    /// The tag name of the payload (e.g., "Greeting").
    pub tag: String,
}

/// Context passed to handlers alongside the payload.
///
/// Provides metadata about the message without giving handlers
/// the ability to forge identity or bypass routing.
#[derive(Debug, Clone)]
pub struct HandlerContext {
    /// Thread UUID for this conversation chain.
    pub thread_id: ThreadId,
    /// Who sent this message (verified by pipeline, not forgeable by handler).
    pub from: AgentId,
    /// This handler's own name (set by pipeline, not self-reported).
    pub own_name: AgentId,
}

/// What a handler can return.
///
/// Handlers express *intent* — the pipeline enforces the rules.
/// A handler can't forge identity: `from` is always overwritten by the pipeline.
/// A handler can't escape peers: the pipeline validates `to` against the peer table.
#[derive(Debug)]
pub enum HandlerResponse {
    /// Send a new message to a named target (forward).
    /// Pipeline will enforce peer constraints and extend the thread chain.
    Send {
        to: AgentId,
        payload_xml: Vec<u8>,
    },

    /// Respond back to caller (prune the thread chain).
    /// Pipeline will look up the chain, prune the last segment,
    /// and route to the previous hop.
    Reply {
        payload_xml: Vec<u8>,
    },

    /// No response — this handler has nothing to say.
    /// If a parent exists in the thread chain, the pipeline synthesizes
    /// an ACK (`<ToolResponse><success>true</success><result>ack</result></ToolResponse>`)
    /// and routes it back so the parent doesn't hang in AwaitingTools.
    /// If no parent exists (chain exhausted), the thread ends silently.
    None,
}

/// The result type handlers return.
pub type HandlerResult = Result<HandlerResponse, crate::error::PipelineError>;

/// The core handler trait.
///
/// Implement this to process messages in the pipeline.
///
/// # Trust Model
///
/// - Handlers receive **validated** payloads (trust earned by pipeline)
/// - Handler responses are **untrusted** (serialized back to raw bytes)
/// - The pipeline enforces identity, routing, and peer constraints
/// - Handlers express intent; the pipeline enforces policy
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Process a validated payload and optionally produce a response.
    ///
    /// The `payload` has already passed XSD validation.
    /// The `ctx` provides verified metadata (thread, sender, own identity).
    ///
    /// Return `HandlerResponse::Send` to forward to a peer,
    /// `HandlerResponse::Reply` to respond to the caller,
    /// or `HandlerResponse::None` to terminate the chain.
    async fn handle(
        &self,
        payload: ValidatedPayload,
        ctx: HandlerContext,
    ) -> HandlerResult;
}

/// Convenience: wrap a closure as a handler.
///
/// Useful for tests and simple handlers that don't need struct state.
pub struct FnHandler<F>(pub F);

#[async_trait]
impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(ValidatedPayload, HandlerContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = HandlerResult> + Send,
{
    async fn handle(
        &self,
        payload: ValidatedPayload,
        ctx: HandlerContext,
    ) -> HandlerResult {
        (self.0)(payload, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fn_handler_works() {
        let handler = FnHandler(|payload: ValidatedPayload, _ctx: HandlerContext| async move {
            // Echo: wrap payload in a response
            Ok(HandlerResponse::Reply {
                payload_xml: payload.xml,
            })
        });

        let payload = ValidatedPayload {
            xml: b"<Greeting><text>hi</text></Greeting>".to_vec(),
            tag: "Greeting".into(),
        };
        let ctx = HandlerContext {
            thread_id: "t1".into(),
            from: "alice".into(),
            own_name: "echo".into(),
        };

        let result = handler.handle(payload, ctx).await.unwrap();
        match result {
            HandlerResponse::Reply { payload_xml } => {
                assert!(String::from_utf8_lossy(&payload_xml).contains("hi"));
            }
            _ => panic!("expected Reply"),
        }
    }
}
