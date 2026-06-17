//! Listener registry — maps payload types to handler instances.
//!
//! The registry is the bridge between routing (which finds the target
//! listener) and dispatch (which calls the handler). It holds the
//! actual `Box<dyn Handler>` instances alongside their metadata.

use std::collections::HashMap;
use std::sync::Arc;

use crate::envelope::AgentId;
use crate::handler::Handler;
use crate::routing::RoutingTable;
use crate::validation::{PayloadSchema, SchemaRegistry};

/// A fully registered listener with its handler and schema.
pub struct RegisteredListener {
    /// Listener name.
    pub name: AgentId,
    /// The handler implementation (trait object).
    pub handler: Arc<dyn Handler>,
    /// Payload tag this listener handles (e.g., "Greeting").
    pub payload_tag: String,
    /// Whether this is an LLM agent.
    pub is_agent: bool,
    /// Declared peers.
    pub peers: Vec<String>,
    /// Description.
    pub description: String,
}

/// Central registry for all listeners, handlers, and schemas.
///
/// Owns the routing table and schema registry. The pipeline uses
/// this to look up handlers during the dispatch stage.
pub struct ListenerRegistry {
    /// Handler instances by listener name.
    handlers: HashMap<String, Arc<dyn Handler>>,
    /// The routing table (for route resolution and peer enforcement).
    pub routing: RoutingTable,
    /// Schema registry (for payload validation).
    pub schemas: SchemaRegistry,
    /// Listener metadata by name.
    listeners: HashMap<String, ListenerMeta>,
}

/// Metadata stored alongside each listener.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields used for introspection in later phases
struct ListenerMeta {
    name: String,
    payload_tag: String,
    is_agent: bool,
    peers: Vec<String>,
    description: String,
}

impl ListenerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            routing: RoutingTable::new(),
            schemas: SchemaRegistry::new(),
            listeners: HashMap::new(),
        }
    }

    /// Register a listener with its handler and optional schema.
    ///
    /// This is the main registration API. It:
    /// 1. Stores the handler instance
    /// 2. Registers the route in the routing table
    /// 3. Optionally registers a payload schema
    pub fn register<H: Handler>(
        &mut self,
        name: &str,
        payload_tag: &str,
        handler: H,
        is_agent: bool,
        peers: Vec<String>,
        description: &str,
        schema: Option<PayloadSchema>,
    ) {
        let handler = Arc::new(handler);
        self.handlers.insert(name.to_string(), handler);

        self.routing
            .register(name, payload_tag, is_agent, peers.clone(), description);

        if let Some(s) = schema {
            self.schemas.register(s);
        }

        self.listeners.insert(
            name.to_string(),
            ListenerMeta {
                name: name.to_string(),
                payload_tag: payload_tag.to_string(),
                is_agent,
                peers,
                description: description.to_string(),
            },
        );
    }

    /// Get a handler by listener name.
    pub fn get_handler(&self, name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(name).cloned()
    }

    /// Get all registered listener names.
    pub fn listener_names(&self) -> Vec<&str> {
        self.listeners.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a listener is registered.
    pub fn has_listener(&self, name: &str) -> bool {
        self.listeners.contains_key(name)
    }

    /// Get listener metadata.
    pub fn listener_info(&self, name: &str) -> Option<(&str, bool, &[String])> {
        self.listeners.get(name).map(|m| {
            (
                m.payload_tag.as_str(),
                m.is_agent,
                m.peers.as_slice(),
            )
        })
    }
}

impl Default for ListenerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{
        HandlerContext, HandlerResponse, HandlerResult, ValidatedPayload,
    };
    use async_trait::async_trait;

    struct EchoHandler;

    #[async_trait]
    impl crate::handler::Handler for EchoHandler {
        async fn handle(&self, payload: ValidatedPayload, _ctx: HandlerContext) -> HandlerResult {
            Ok(HandlerResponse::Reply {
                payload: payload.to_payload(),
            })
        }
    }

    #[test]
    fn register_and_retrieve() {
        let mut reg = ListenerRegistry::new();
        reg.register(
            "echo",
            "Greeting",
            EchoHandler,
            false,
            vec![],
            "Echo handler",
            None,
        );

        assert!(reg.has_listener("echo"));
        assert!(reg.get_handler("echo").is_some());
        assert!(reg.get_handler("nobody").is_none());
    }

    #[test]
    fn routing_works_through_registry() {
        let mut reg = ListenerRegistry::new();
        reg.register(
            "greeter",
            "Greeting",
            EchoHandler,
            true,
            vec!["shouter".into()],
            "Greets",
            None,
        );

        let entries = reg.routing.resolve(Some("greeter"), "Greeting").unwrap();
        assert_eq!(entries[0].name, "greeter");
    }
}
