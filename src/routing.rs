//! Routing table and peer enforcement.
//!
//! The routing table maps `to_id.payload_tag` keys to registered handlers.
//! Peer enforcement ensures agents can only message declared peers —
//! even if a handler tries to forge a target, the pipeline catches it.

use std::collections::{HashMap, HashSet};

use crate::envelope::AgentId;
use crate::error::{PipelineError, PipelineResult};

/// A registered listener entry in the routing table.
#[derive(Debug, Clone)]
pub struct ListenerEntry {
    /// Listener name (unique identifier).
    pub name: AgentId,
    /// Whether this listener is an LLM agent (subject to peer constraints).
    pub is_agent: bool,
    /// Declared peers — agents this listener is allowed to message.
    /// Empty means no peer restriction (for non-agent listeners).
    pub peers: HashSet<String>,
    /// The routing key for this listener: `name.payload_tag`.
    pub route_key: String,
    /// Human-readable description.
    pub description: String,
}

/// Routes messages to handlers and enforces peer constraints.
///
/// The routing table is built at config time and is immutable during
/// pipeline operation. Adding/removing listeners requires a config reload.
#[derive(Debug, Default)]
pub struct RoutingTable {
    /// route_key → listener entries (can have multiple for broadcast)
    routes: HashMap<String, Vec<ListenerEntry>>,
    /// listener name → listener entry (for quick lookup by name)
    by_name: HashMap<String, ListenerEntry>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a listener in the routing table.
    pub fn register(
        &mut self,
        name: &str,
        payload_tag: &str,
        is_agent: bool,
        peers: Vec<String>,
        description: &str,
    ) {
        let route_key = format!("{}.{}", name.to_lowercase(), payload_tag.to_lowercase());

        let entry = ListenerEntry {
            name: name.to_string(),
            is_agent,
            peers: peers.into_iter().collect(),
            route_key: route_key.clone(),
            description: description.to_string(),
        };

        self.routes
            .entry(route_key)
            .or_default()
            .push(entry.clone());
        self.by_name.insert(name.to_string(), entry);
    }

    /// Look up a listener by name.
    pub fn get_by_name(&self, name: &str) -> Option<&ListenerEntry> {
        self.by_name.get(name)
    }

    /// Resolve routing for a message.
    ///
    /// Builds the route key from `to_id` and `payload_tag`, then
    /// looks up registered listeners.
    pub fn resolve(
        &self,
        to_id: Option<&str>,
        payload_tag: &str,
    ) -> PipelineResult<&[ListenerEntry]> {
        let tag_lower = payload_tag.to_lowercase();

        let route_key = if let Some(to) = to_id {
            format!("{}.{}", to.to_lowercase(), tag_lower)
        } else {
            tag_lower
        };

        match self.routes.get(&route_key) {
            Some(entries) if !entries.is_empty() => Ok(entries),
            _ => Err(PipelineError::NoRoute(route_key)),
        }
    }

    /// Enforce peer constraints.
    ///
    /// Checks that `from_agent` is allowed to send to `to_agent`.
    /// Non-agent senders are always allowed (system messages, etc.).
    pub fn enforce_peers(&self, from_agent: &str, to_agent: &str) -> PipelineResult<()> {
        let sender = match self.by_name.get(from_agent) {
            Some(entry) => entry,
            None => return Ok(()), // unknown sender (e.g., "system") → always allowed
        };

        // Non-agents have no peer restrictions
        if !sender.is_agent || sender.peers.is_empty() {
            return Ok(());
        }

        if sender.peers.contains(to_agent) {
            Ok(())
        } else {
            Err(PipelineError::PeerViolation {
                from: from_agent.to_string(),
                to: to_agent.to_string(),
                allowed: sender.peers.iter().cloned().collect(),
            })
        }
    }

    /// Get all registered listener names.
    pub fn listener_names(&self) -> Vec<&str> {
        self.by_name.keys().map(|s| s.as_str()).collect()
    }

    /// Get all route keys.
    pub fn route_keys(&self) -> Vec<&str> {
        self.routes.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table() -> RoutingTable {
        let mut table = RoutingTable::new();
        table.register(
            "greeter",
            "Greeting",
            true,
            vec!["shouter".into()],
            "Greeting agent",
        );
        table.register(
            "shouter",
            "GreetingResponse",
            false,
            vec![],
            "Shouts in caps",
        );
        table
    }

    #[test]
    fn resolve_by_name_and_tag() {
        let table = sample_table();
        let entries = table.resolve(Some("greeter"), "Greeting").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "greeter");
    }

    #[test]
    fn resolve_missing_route() {
        let table = sample_table();
        let err = table.resolve(Some("nobody"), "Ping").unwrap_err();
        assert!(matches!(err, PipelineError::NoRoute(_)));
    }

    #[test]
    fn peer_enforcement_allowed() {
        let table = sample_table();
        // greeter → shouter: allowed (declared peer)
        table.enforce_peers("greeter", "shouter").unwrap();
    }

    #[test]
    fn peer_enforcement_blocked() {
        let table = sample_table();
        // greeter → secret: NOT allowed
        let err = table.enforce_peers("greeter", "secret").unwrap_err();
        match err {
            PipelineError::PeerViolation { from, to, allowed } => {
                assert_eq!(from, "greeter");
                assert_eq!(to, "secret");
                assert!(allowed.contains(&"shouter".to_string()));
            }
            _ => panic!("expected PeerViolation"),
        }
    }

    #[test]
    fn non_agent_no_restrictions() {
        let table = sample_table();
        // shouter is not an agent → can message anyone
        table.enforce_peers("shouter", "greeter").unwrap();
    }

    #[test]
    fn unknown_sender_allowed() {
        let table = sample_table();
        // "system" is not registered → always allowed
        table.enforce_peers("system", "greeter").unwrap();
    }

    #[test]
    fn get_by_name() {
        let table = sample_table();
        let entry = table.get_by_name("greeter").unwrap();
        assert!(entry.is_agent);
        assert!(entry.peers.contains("shouter"));
    }
}
