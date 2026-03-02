//! Composable middleware for the dispatch stage.
//!
//! Middleware intercepts messages before and after handler dispatch,
//! enabling cross-cutting concerns (loop guards, permission gates, logging)
//! without polluting handler logic.
//!
//! # Execution Order
//!
//! Pre-dispatch runs in registration order (first registered, first called).
//! Post-dispatch runs in reverse order (last registered, first called).
//! This gives an "onion" wrapping: the first middleware added is the outermost layer.
//!
//! ```text
//! pre[0] → pre[1] → handler → post[1] → post[0]
//! ```

use async_trait::async_trait;

use crate::envelope::AgentId;
use crate::error::PipelineError;
use crate::handler::{HandlerResponse, ValidatedPayload};

/// Metadata about the message being dispatched.
///
/// Built from the routed message envelope — middleware sees the same
/// verified fields that the handler would receive via `HandlerContext`.
#[derive(Debug, Clone)]
pub struct DispatchMeta {
    /// Who sent this message (verified by pipeline).
    pub from: AgentId,
    /// The resolved dispatch target.
    pub to: AgentId,
    /// Thread UUID for this conversation chain.
    pub thread_id: String,
    /// The payload tag name (e.g., "ToolResponse", "AgentTask").
    pub payload_tag: String,
}

/// Pre-dispatch decision.
#[derive(Debug)]
pub enum PreDispatchVerdict {
    /// Allow dispatch to proceed to the next middleware / handler.
    Continue,
    /// Skip the handler entirely and return this response.
    ShortCircuit(HandlerResponse),
}

/// Post-dispatch decision.
#[derive(Debug)]
pub enum PostDispatchVerdict {
    /// Pass the handler's response through unchanged.
    PassThrough(HandlerResponse),
    /// Replace the handler's response with a different one.
    Replace(HandlerResponse),
}

/// Middleware that can intercept messages before and/or after handler dispatch.
///
/// Default implementations pass through — override only what you need.
#[async_trait]
pub trait Middleware: Send + Sync + 'static {
    /// Called before the handler processes the message.
    ///
    /// Return `Continue` to proceed, or `ShortCircuit` to skip the handler
    /// and return a response directly.
    async fn pre_dispatch(
        &self,
        _meta: &DispatchMeta,
        _payload: &ValidatedPayload,
    ) -> Result<PreDispatchVerdict, PipelineError> {
        Ok(PreDispatchVerdict::Continue)
    }

    /// Called after the handler has produced a response.
    ///
    /// Return `PassThrough` to keep the response, or `Replace` to substitute
    /// a different one. Post-dispatch runs in reverse registration order.
    async fn post_dispatch(
        &self,
        _meta: &DispatchMeta,
        _payload: &ValidatedPayload,
        response: HandlerResponse,
    ) -> Result<PostDispatchVerdict, PipelineError> {
        Ok(PostDispatchVerdict::PassThrough(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No-op middleware: both hooks pass through.
    struct NoOp;

    #[async_trait]
    impl Middleware for NoOp {}

    /// Short-circuit middleware: always blocks dispatch.
    struct AlwaysBlock;

    #[async_trait]
    impl Middleware for AlwaysBlock {
        async fn pre_dispatch(
            &self,
            _meta: &DispatchMeta,
            _payload: &ValidatedPayload,
        ) -> Result<PreDispatchVerdict, PipelineError> {
            Ok(PreDispatchVerdict::ShortCircuit(HandlerResponse::Reply {
                payload_xml: b"<Blocked/>".to_vec(),
            }))
        }
    }

    fn test_meta() -> DispatchMeta {
        DispatchMeta {
            from: "alice".into(),
            to: "bob".into(),
            thread_id: "t1".into(),
            payload_tag: "Greeting".into(),
        }
    }

    fn test_payload() -> ValidatedPayload {
        ValidatedPayload {
            xml: b"<Greeting><text>hi</text></Greeting>".to_vec(),
            tag: "Greeting".into(),
        }
    }

    #[tokio::test]
    async fn noop_pre_dispatch_continues() {
        let mw = NoOp;
        let result = mw.pre_dispatch(&test_meta(), &test_payload()).await.unwrap();
        assert!(matches!(result, PreDispatchVerdict::Continue));
    }

    #[tokio::test]
    async fn noop_post_dispatch_passes_through() {
        let mw = NoOp;
        let response = HandlerResponse::None;
        let result = mw
            .post_dispatch(&test_meta(), &test_payload(), response)
            .await
            .unwrap();
        assert!(matches!(result, PostDispatchVerdict::PassThrough(HandlerResponse::None)));
    }

    #[tokio::test]
    async fn always_block_short_circuits() {
        let mw = AlwaysBlock;
        let result = mw.pre_dispatch(&test_meta(), &test_payload()).await.unwrap();
        match result {
            PreDispatchVerdict::ShortCircuit(HandlerResponse::Reply { payload_xml }) => {
                assert_eq!(payload_xml, b"<Blocked/>");
            }
            _ => panic!("expected ShortCircuit"),
        }
    }

    #[tokio::test]
    async fn post_dispatch_can_replace() {
        struct Replacer;

        #[async_trait]
        impl Middleware for Replacer {
            async fn post_dispatch(
                &self,
                _meta: &DispatchMeta,
                _payload: &ValidatedPayload,
                _response: HandlerResponse,
            ) -> Result<PostDispatchVerdict, PipelineError> {
                Ok(PostDispatchVerdict::Replace(HandlerResponse::Reply {
                    payload_xml: b"<Replaced/>".to_vec(),
                }))
            }
        }

        let mw = Replacer;
        let response = HandlerResponse::None;
        let result = mw
            .post_dispatch(&test_meta(), &test_payload(), response)
            .await
            .unwrap();
        match result {
            PostDispatchVerdict::Replace(HandlerResponse::Reply { payload_xml }) => {
                assert_eq!(payload_xml, b"<Replaced/>");
            }
            _ => panic!("expected Replace"),
        }
    }
}
