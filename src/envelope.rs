//! Shared identifiers and the envelope namespace.
//!
//! The envelope *type* now lives in [`crate::wire`] (the canonical WIT-projected
//! [`crate::wire::Envelope`]); its serialization lives in [`crate::codec`]. What remains
//! here are the cross-cutting type aliases and the wire namespace constant.

/// The envelope namespace URI (matches xml-pipeline; used by the transitional XML codec).
pub const ENVELOPE_NS: &str = "https://xml-pipeline.org/ns/envelope/v1";

/// Unique agent identifier (listener name). Used by routing/registry, which are
/// name-keyed; the canonical addressable form is [`crate::wire::Address`].
pub type AgentId = String;

/// Opaque thread identifier (UUID string).
pub type ThreadId = String;
