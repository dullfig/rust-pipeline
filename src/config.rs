//! YAML configuration loading.
//!
//! Loads organism configuration from YAML files (same format as
//! xml-pipeline's `organism.yaml`). Serde handles deserialization;
//! the pipeline builder uses this to register listeners and set up routing.

use std::path::Path;

use serde::Deserialize;

use crate::error::{PipelineError, PipelineResult};

/// Top-level organism configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    /// Organism identity.
    #[serde(default)]
    pub organism: OrganismConfig,

    /// Maximum concurrent messages in the pipeline.
    #[serde(default = "default_max_concurrent_pipelines")]
    pub max_concurrent_pipelines: usize,

    /// Maximum concurrent handler invocations.
    #[serde(default = "default_max_concurrent_handlers")]
    pub max_concurrent_handlers: usize,

    /// Per-agent concurrency limit.
    #[serde(default = "default_max_concurrent_per_agent")]
    pub max_concurrent_per_agent: usize,

    /// Thread scheduling strategy.
    #[serde(default = "default_thread_scheduling")]
    pub thread_scheduling: String,

    /// Listener definitions.
    #[serde(default)]
    pub listeners: Vec<ListenerConfig>,
}

/// Organism identity section.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct OrganismConfig {
    /// Organism name.
    #[serde(default = "default_organism_name")]
    pub name: String,

    /// WebSocket port.
    #[serde(default = "default_port")]
    pub port: u16,
}

/// Listener configuration from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct ListenerConfig {
    /// Unique listener name.
    pub name: String,

    /// Import path to the payload class (e.g., "handlers.hello.Greeting").
    /// In Rust, this maps to a payload tag name for routing.
    pub payload_class: String,

    /// Import path to the handler (e.g., "handlers.hello.handle_greeting").
    /// In Rust, handlers are registered programmatically; this is metadata.
    pub handler: String,

    /// Human-readable description.
    pub description: String,

    /// Whether this listener is an LLM agent.
    #[serde(default)]
    pub agent: bool,

    /// Declared peers (agents this listener can message).
    #[serde(default)]
    pub peers: Vec<String>,

    /// Whether to broadcast to all matching listeners.
    #[serde(default)]
    pub broadcast: bool,

    /// System prompt for LLM agents.
    #[serde(default)]
    pub prompt: String,
}

impl ListenerConfig {
    /// Extract the payload tag from the payload_class path.
    ///
    /// e.g., "handlers.hello.Greeting" → "Greeting"
    pub fn payload_tag(&self) -> &str {
        self.payload_class
            .rsplit('.')
            .next()
            .unwrap_or(&self.payload_class)
    }

    /// Extract the handler function name from the handler path.
    ///
    /// e.g., "handlers.hello.handle_greeting" → "handle_greeting"
    pub fn handler_name(&self) -> &str {
        self.handler
            .rsplit('.')
            .next()
            .unwrap_or(&self.handler)
    }
}

/// Load a pipeline configuration from a YAML file.
pub fn load_config(path: &Path) -> PipelineResult<PipelineConfig> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| PipelineError::Config(format!("failed to read {}: {e}", path.display())))?;

    parse_config(&contents)
}

/// Parse a pipeline configuration from a YAML string.
pub fn parse_config(yaml: &str) -> PipelineResult<PipelineConfig> {
    serde_yaml::from_str(yaml)
        .map_err(|e| PipelineError::Config(format!("YAML parse error: {e}")))
}

// ── Defaults ─────────────────────────────────────────────────────────

fn default_max_concurrent_pipelines() -> usize {
    50
}
fn default_max_concurrent_handlers() -> usize {
    20
}
fn default_max_concurrent_per_agent() -> usize {
    5
}
fn default_thread_scheduling() -> String {
    "breadth-first".into()
}
fn default_organism_name() -> String {
    "unnamed".into()
}
fn default_port() -> u16 {
    8765
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
organism:
  name: test-org
listeners: []
"#;
        let config = parse_config(yaml).unwrap();
        assert_eq!(config.organism.name, "test-org");
        assert!(config.listeners.is_empty());
    }

    #[test]
    fn parse_full_config() {
        let yaml = r#"
organism:
  name: hello-world
  port: 8765

max_concurrent_pipelines: 50
max_concurrent_handlers: 20

listeners:
  - name: greeter
    payload_class: handlers.hello.Greeting
    handler: handlers.hello.handle_greeting
    description: Greeting agent
    agent: true
    peers: [shouter]
    prompt: |
      You are a friendly greeter.

  - name: shouter
    payload_class: handlers.hello.GreetingResponse
    handler: handlers.hello.handle_shout
    description: Shouts in caps
"#;
        let config = parse_config(yaml).unwrap();
        assert_eq!(config.organism.name, "hello-world");
        assert_eq!(config.listeners.len(), 2);

        let greeter = &config.listeners[0];
        assert_eq!(greeter.name, "greeter");
        assert!(greeter.agent);
        assert_eq!(greeter.peers, vec!["shouter"]);
        assert_eq!(greeter.payload_tag(), "Greeting");

        let shouter = &config.listeners[1];
        assert!(!shouter.agent);
        assert!(shouter.peers.is_empty());
    }

    #[test]
    fn defaults_applied() {
        let yaml = "organism: { name: test }";
        let config = parse_config(yaml).unwrap();
        assert_eq!(config.max_concurrent_pipelines, 50);
        assert_eq!(config.max_concurrent_handlers, 20);
        assert_eq!(config.organism.port, 8765);
    }

    #[test]
    fn payload_tag_extraction() {
        let lc = ListenerConfig {
            name: "test".into(),
            payload_class: "handlers.hello.Greeting".into(),
            handler: "handlers.hello.handle_greeting".into(),
            description: "test".into(),
            agent: false,
            peers: vec![],
            broadcast: false,
            prompt: String::new(),
        };
        assert_eq!(lc.payload_tag(), "Greeting");
        assert_eq!(lc.handler_name(), "handle_greeting");
    }

    #[test]
    fn invalid_yaml_error() {
        let err = parse_config("{{{{invalid").unwrap_err();
        assert!(matches!(err, PipelineError::Config(_)));
    }
}
