//! Pipeline error types.
//!
//! Every stage in the pipeline produces `Result<T, PipelineError>`,
//! giving us natural short-circuiting through the trust progression.

use thiserror::Error;

/// The unified error type for all pipeline stages.
///
/// Each variant corresponds to a stage where trust can fail to be established.
/// Raw bytes enter the pipeline and must earn their way to dispatch — any
/// failure at any stage produces a specific, diagnosable error.
#[derive(Debug, Error)]
pub enum PipelineError {
    // ── Ingress / Repair ────────────────────────────────────────────
    /// Raw bytes couldn't be repaired into valid XML.
    #[error("repair failed: {0}")]
    Repair(String),

    // ── Parse ───────────────────────────────────────────────────────
    /// Well-formed XML, but not a valid envelope structure.
    #[error("envelope parse error: {0}")]
    EnvelopeParse(String),

    /// Missing required metadata field.
    #[error("missing meta field: {0}")]
    MissingMeta(&'static str),

    // ── Validation ──────────────────────────────────────────────────
    /// Payload failed XSD schema validation.
    #[error("validation failed: {0}")]
    Validation(String),

    // ── Routing ─────────────────────────────────────────────────────
    /// No listener registered for this payload type / target.
    #[error("no route for: {0}")]
    NoRoute(String),

    /// Agent attempted to message a non-declared peer.
    #[error("peer violation: {from} cannot message {to} (allowed: {allowed:?})")]
    PeerViolation {
        from: String,
        to: String,
        allowed: Vec<String>,
    },

    // ── Thread ──────────────────────────────────────────────────────
    /// Thread UUID not found in registry.
    #[error("unknown thread: {0}")]
    UnknownThread(String),

    /// Thread chain exhausted (response with nowhere to go).
    #[error("thread chain exhausted: {0}")]
    ChainExhausted(String),

    // ── Handler ─────────────────────────────────────────────────────
    /// Handler returned an error.
    #[error("handler error: {0}")]
    Handler(String),

    /// Handler panicked (caught by tokio).
    #[error("handler panicked: {0}")]
    HandlerPanic(String),

    // ── Config ──────────────────────────────────────────────────────
    /// Configuration file couldn't be loaded or parsed.
    #[error("config error: {0}")]
    Config(String),

    // ── Serialization ───────────────────────────────────────────────
    /// Failed to serialize handler response back to bytes.
    #[error("serialization error: {0}")]
    Serialization(String),

    // ── XML ─────────────────────────────────────────────────────────
    /// Underlying XML parsing error.
    #[error("xml error: {0}")]
    Xml(#[from] quick_xml::Error),
}

/// Alias used throughout the crate.
pub type PipelineResult<T> = Result<T, PipelineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = PipelineError::PeerViolation {
            from: "greeter".into(),
            to: "secret-agent".into(),
            allowed: vec!["shouter".into()],
        };
        assert!(e.to_string().contains("greeter"));
        assert!(e.to_string().contains("secret-agent"));
    }

    #[test]
    fn error_from_quick_xml() {
        let xml_err = quick_xml::Error::Syntax(quick_xml::errors::SyntaxError::InvalidBangMarkup);
        let pipeline_err: PipelineError = xml_err.into();
        assert!(matches!(pipeline_err, PipelineError::Xml(_)));
    }
}
