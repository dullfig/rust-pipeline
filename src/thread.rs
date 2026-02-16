//! Thread registry — maps opaque UUIDs to call chains.
//!
//! Call chains track the path a message has taken through the system:
//!   A calls B → chain: "a.b"
//!   B calls C → chain: "a.b.c"
//!
//! UUIDs obscure the topology from agents. They only see an opaque
//! `thread_id`, not the actual call chain.
//!
//! Response routing:
//!   When an agent returns a Reply, the registry:
//!   1. Looks up the UUID to get the chain
//!   2. Prunes the last segment (the responder)
//!   3. Routes to the new last segment (the caller)
//!   4. Returns the new UUID for the pruned chain

use std::collections::HashMap;

use uuid::Uuid;

use crate::envelope::ThreadId;

/// Bidirectional mapping between UUIDs and dot-separated call chains.
///
/// Thread-safe when accessed through the pipeline's single-threaded
/// dispatch stage. For shared access, wrap in `Arc<Mutex<_>>`.
#[derive(Debug, Default)]
pub struct ThreadRegistry {
    /// chain string → UUID
    chain_to_uuid: HashMap<String, String>,
    /// UUID → chain string
    uuid_to_chain: HashMap<String, String>,
    /// Root thread UUID (set at boot)
    root_uuid: Option<String>,
    /// Root chain string
    root_chain: String,
}

/// Result of pruning a thread chain for a response.
#[derive(Debug, PartialEq)]
pub struct PruneResult {
    /// The target agent (new last segment after pruning).
    pub target: String,
    /// The UUID for the pruned chain.
    pub thread_id: ThreadId,
}

impl ThreadRegistry {
    pub fn new() -> Self {
        Self {
            chain_to_uuid: HashMap::new(),
            uuid_to_chain: HashMap::new(),
            root_uuid: None,
            root_chain: "system".into(),
        }
    }

    /// Initialize the root thread at boot time.
    ///
    /// Must be called once at startup. The root thread is the
    /// ancestor of all other threads.
    pub fn initialize_root(&mut self, organism_name: &str) -> String {
        if let Some(ref uuid) = self.root_uuid {
            return uuid.clone();
        }

        self.root_chain = format!("system.{organism_name}");
        let uuid = Uuid::new_v4().to_string();
        self.chain_to_uuid
            .insert(self.root_chain.clone(), uuid.clone());
        self.uuid_to_chain
            .insert(uuid.clone(), self.root_chain.clone());
        self.root_uuid = Some(uuid.clone());
        uuid
    }

    /// Get the root thread UUID.
    pub fn root_uuid(&self) -> Option<&str> {
        self.root_uuid.as_deref()
    }

    /// Look up chain for a UUID.
    pub fn lookup(&self, thread_id: &str) -> Option<&str> {
        self.uuid_to_chain.get(thread_id).map(|s| s.as_str())
    }

    /// Get existing UUID for chain, or create new one.
    pub fn get_or_create(&mut self, chain: &str) -> String {
        if let Some(uuid) = self.chain_to_uuid.get(chain) {
            return uuid.clone();
        }

        let uuid = Uuid::new_v4().to_string();
        self.chain_to_uuid.insert(chain.to_string(), uuid.clone());
        self.uuid_to_chain.insert(uuid.clone(), chain.to_string());
        uuid
    }

    /// Start a new call chain between initiator and target.
    pub fn start_chain(&mut self, initiator: &str, target: &str) -> String {
        let chain = format!("{initiator}.{target}");
        self.get_or_create(&chain)
    }

    /// Register an existing UUID to a call chain.
    ///
    /// Used when external messages arrive with a pre-assigned UUID
    /// that isn't in the registry yet.
    pub fn register_thread(
        &mut self,
        thread_id: &str,
        initiator: &str,
        target: &str,
    ) -> String {
        // Already registered?
        if self.uuid_to_chain.contains_key(thread_id) {
            return thread_id.to_string();
        }

        // Build chain rooted at system root
        let chain = if self.root_uuid.is_some() {
            format!("{}.{initiator}.{target}", self.root_chain)
        } else {
            format!("{initiator}.{target}")
        };

        // Chain already has a different UUID?
        if let Some(existing) = self.chain_to_uuid.get(&chain) {
            return existing.clone();
        }

        // Register
        self.chain_to_uuid
            .insert(chain.clone(), thread_id.to_string());
        self.uuid_to_chain
            .insert(thread_id.to_string(), chain);
        thread_id.to_string()
    }

    /// Extend a chain with a new hop and get UUID for the extended chain.
    ///
    /// e.g., chain "a.b" + hop "c" → "a.b.c" with a new UUID.
    pub fn extend_chain(&mut self, current_uuid: &str, next_hop: &str) -> String {
        let current_chain = self
            .uuid_to_chain
            .get(current_uuid)
            .cloned()
            .unwrap_or_default();

        let new_chain = if current_chain.is_empty() {
            next_hop.to_string()
        } else {
            format!("{current_chain}.{next_hop}")
        };

        // Extended chain already exists?
        if let Some(uuid) = self.chain_to_uuid.get(&new_chain) {
            return uuid.clone();
        }

        let uuid = Uuid::new_v4().to_string();
        self.chain_to_uuid.insert(new_chain.clone(), uuid.clone());
        self.uuid_to_chain.insert(uuid.clone(), new_chain);
        uuid
    }

    /// Prune chain for a response and get the target.
    ///
    /// When an agent responds:
    /// 1. Look up the chain
    /// 2. Remove the last segment (the responder)
    /// 3. Return the new target (new last segment) and new UUID
    ///
    /// Returns `None` if the chain is exhausted (no one to respond to).
    pub fn prune_for_response(&mut self, thread_id: &str) -> Option<PruneResult> {
        let chain = self.uuid_to_chain.get(thread_id)?.clone();

        let parts: Vec<&str> = chain.split('.').collect();
        if parts.len() <= 1 {
            // Chain exhausted — clean up
            self.cleanup(thread_id);
            return None;
        }

        // Prune last segment
        let pruned_parts = &parts[..parts.len() - 1];
        let target = pruned_parts.last().unwrap().to_string();
        let pruned_chain: String = pruned_parts.join(".");

        // Get or create UUID for pruned chain
        let new_uuid = if let Some(uuid) = self.chain_to_uuid.get(&pruned_chain) {
            uuid.clone()
        } else {
            let uuid = Uuid::new_v4().to_string();
            self.chain_to_uuid
                .insert(pruned_chain.clone(), uuid.clone());
            self.uuid_to_chain.insert(uuid.clone(), pruned_chain);
            uuid
        };

        Some(PruneResult {
            target,
            thread_id: new_uuid,
        })
    }

    /// Explicitly clean up a thread UUID.
    pub fn cleanup(&mut self, thread_id: &str) {
        if let Some(chain) = self.uuid_to_chain.remove(thread_id) {
            self.chain_to_uuid.remove(&chain);
        }
    }

    /// Return current mappings for debugging.
    pub fn debug_dump(&self) -> &HashMap<String, String> {
        &self.uuid_to_chain
    }

    /// Clear all thread mappings.
    pub fn clear(&mut self) {
        self.chain_to_uuid.clear();
        self.uuid_to_chain.clear();
        self.root_uuid = None;
        self.root_chain = "system".into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_root() {
        let mut reg = ThreadRegistry::new();
        let uuid = reg.initialize_root("hello-world");
        assert!(!uuid.is_empty());
        assert_eq!(reg.lookup(&uuid), Some("system.hello-world"));
    }

    #[test]
    fn start_and_extend_chain() {
        let mut reg = ThreadRegistry::new();
        let t1 = reg.start_chain("console", "router");
        assert_eq!(reg.lookup(&t1), Some("console.router"));

        let t2 = reg.extend_chain(&t1, "greeter");
        assert_eq!(reg.lookup(&t2), Some("console.router.greeter"));
        assert_ne!(t1, t2);
    }

    #[test]
    fn prune_chain() {
        let mut reg = ThreadRegistry::new();
        let t1 = reg.start_chain("console", "router");
        let t2 = reg.extend_chain(&t1, "greeter");

        // greeter responds → prune to "console.router", target = "router"
        let result = reg.prune_for_response(&t2).unwrap();
        assert_eq!(result.target, "router");
        assert_eq!(reg.lookup(&result.thread_id), Some("console.router"));
    }

    #[test]
    fn prune_exhausted() {
        let mut reg = ThreadRegistry::new();
        let uuid = reg.get_or_create("console");

        // Single-segment chain — nowhere to prune to
        assert!(reg.prune_for_response(&uuid).is_none());
    }

    #[test]
    fn register_external_thread() {
        let mut reg = ThreadRegistry::new();
        reg.initialize_root("org");

        let external_uuid = "ext-uuid-123";
        let registered = reg.register_thread(external_uuid, "console", "router");
        assert_eq!(registered, external_uuid);
        assert_eq!(
            reg.lookup(external_uuid),
            Some("system.org.console.router")
        );
    }

    #[test]
    fn idempotent_get_or_create() {
        let mut reg = ThreadRegistry::new();
        let u1 = reg.get_or_create("a.b.c");
        let u2 = reg.get_or_create("a.b.c");
        assert_eq!(u1, u2);
    }

    #[test]
    fn cleanup_removes_both_directions() {
        let mut reg = ThreadRegistry::new();
        let uuid = reg.get_or_create("test.chain");
        assert!(reg.lookup(&uuid).is_some());

        reg.cleanup(&uuid);
        assert!(reg.lookup(&uuid).is_none());
    }
}
