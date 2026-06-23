//! The switchboard — the single materialize-then-inject front for a Pipeline.
//!
//! Everything that puts a message *into* a pipeline for an addressed instance goes
//! through here: external entry (HTTP/triggers), cross-instance dispatch, and the
//! front door. `route(envelope)` resolves the destination instance (materializing it on
//! first access via the [`Materializer`] hook), stamps the resolved delivery thread, and
//! injects the **canonical** [`Envelope`] into the pipeline ingress.
//!
//! This collapses the ad-hoc duplication it replaces — there is **one** materialization
//! story (the hook), **one** envelope (no second `platform::Envelope`), and **no bridge**
//! that collapses the address: the canonical envelope's hierarchical [`Address`] carries
//! namespace / organism / key / buffer intact.
//!
//! Topology note (§8.0.1): the switchboard fronts ONE pipeline and routes to many
//! addressed *instances* (kernel threads sharing listeners) — not many pipelines.
//! The rich materialization machinery (VMM tiers, eviction, snapshots, buffers) lives in
//! the host behind the hook; the switchboard owns only the resolve-then-inject sequencing.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::codec::encode_envelope;
use crate::error::PipelineError;
use crate::wire::{Address, Envelope};

/// Resolves an instance address to its delivery thread id, materializing the instance
/// (organism lookup + state allocation) on first access.
///
/// This is the ONE materialization story. The host (e.g. agentos's `InstanceRegistry` +
/// kernel) implements it; the switchboard calls [`Materializer::resolve`] before every
/// inject, so all injection paths funnel through a single materialization point.
#[async_trait]
pub trait Materializer: Send + Sync {
    /// Resolve `address` (instance + optional buffer) to its delivery thread id,
    /// materializing the instance — and buffer sub-thread, if any — as needed.
    ///
    /// Returns `Err` if the organism is unknown / not materializable. The mapping is the
    /// host's to own (it holds the rich per-instance state); the switchboard only needs
    /// the resulting thread id.
    async fn resolve(&self, address: &Address) -> Result<String, String>;

    /// Evict an instance by its delivery thread id (idle sweep or explicit kill).
    /// Default: no-op.
    async fn evict(&self, _thread_id: &str) -> Result<(), String> {
        Ok(())
    }
}

/// Errors from the switchboard.
#[derive(Debug, thiserror::Error)]
pub enum SwitchboardError {
    /// The envelope carried no `to` address — nowhere to route.
    #[error("envelope has no destination address")]
    NoDestination,
    /// The materializer could not resolve/materialize the destination.
    #[error("materialization failed: {0}")]
    Materialize(String),
    /// The pipeline ingress channel was closed.
    #[error("pipeline ingress closed")]
    IngressClosed,
    /// Failed to encode the envelope for injection.
    #[error(transparent)]
    Encode(#[from] PipelineError),
}

/// The switchboard: a materialize-then-inject front over a Pipeline's ingress.
///
/// Holds a [`Materializer`] hook and the pipeline's ingress sender. It is deliberately
/// thin — resolve, stamp, inject — because the heavy lifting (VMM lifecycle, kernel
/// state) is the host's, behind the hook.
pub struct Switchboard<M: Materializer> {
    materializer: M,
    ingress: mpsc::Sender<Vec<u8>>,
}

impl<M: Materializer> Switchboard<M> {
    /// Create a switchboard over a pipeline `ingress`, using `materializer` to resolve
    /// destination instances.
    pub fn new(materializer: M, ingress: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            materializer,
            ingress,
        }
    }

    /// Borrow the materializer hook (for eviction sweeps, introspection).
    pub fn materializer(&self) -> &M {
        &self.materializer
    }

    /// Materialize the destination instance and inject the canonical envelope.
    ///
    /// 1. Resolve `envelope.meta.to` to a delivery thread (materializing if new).
    /// 2. Stamp that thread onto the envelope (the listener loads instance context by it).
    /// 3. Encode and inject the canonical envelope into the pipeline ingress.
    ///
    /// On materialization failure nothing is injected (fail-closed).
    pub async fn route(&self, mut envelope: Envelope) -> Result<(), SwitchboardError> {
        let to = envelope
            .meta
            .to
            .as_ref()
            .ok_or(SwitchboardError::NoDestination)?;

        let thread_id = self
            .materializer
            .resolve(to)
            .await
            .map_err(SwitchboardError::Materialize)?;

        envelope.meta.thread = thread_id;

        let bytes = encode_envelope(&envelope)?;
        self.ingress
            .send(bytes)
            .await
            .map_err(|_| SwitchboardError::IngressClosed)?;
        Ok(())
    }
}

/// The switchboard is the local-delivery sink for the federation server: an inbound
/// envelope (from a peer node) is materialized + injected just like any other.
#[async_trait]
impl<M: Materializer> crate::federation::LocalDelivery for Switchboard<M> {
    async fn deliver(&self, envelope: Envelope) -> Result<(), String> {
        self.route(envelope).await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_envelope;
    use crate::wire::{Meta, Payload, Provenance};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// A test materializer: assigns sequential `inst-NNNNNN` ids keyed by *instance*
    /// address (buffer dropped, so `bob[alice]` and `bob[alice].dm` share an instance),
    /// reuses the id on repeat, and rejects organism "nobody".
    #[derive(Clone, Default)]
    struct TestMat {
        table: Arc<Mutex<HashMap<String, String>>>,
        materialized: Arc<Mutex<Vec<String>>>,
        next: Arc<Mutex<u64>>,
    }

    #[async_trait]
    impl Materializer for TestMat {
        async fn resolve(&self, address: &Address) -> Result<String, String> {
            if address.organism() == Some("nobody") {
                return Err("unknown organism".into());
            }
            let inst = address.instance_address().to_string();
            let mut table = self.table.lock().await;
            if let Some(t) = table.get(&inst) {
                return Ok(t.clone());
            }
            let mut n = self.next.lock().await;
            *n += 1;
            let tid = format!("inst-{:06}", *n);
            table.insert(inst.clone(), tid.clone());
            self.materialized.lock().await.push(inst);
            Ok(tid)
        }
    }

    fn env_to(to: &str) -> Envelope {
        Envelope {
            meta: Meta {
                from: "ext".into(),
                to: Some(Address::parse(to).unwrap()),
                thread: "unset".into(),
                provenance: Provenance::EMPTY,
            },
            payload: Payload::single("Greeting", "text", "hi"),
        }
    }

    #[tokio::test]
    async fn materializes_then_injects() {
        let (tx, mut rx) = mpsc::channel(8);
        let mat = TestMat::default();
        let materialized = mat.materialized.clone();
        let sb = Switchboard::new(mat, tx);

        sb.route(env_to("ringhub.bob[alice].dm")).await.unwrap();

        // The instance (buffer dropped) was materialized exactly once.
        assert_eq!(
            *materialized.lock().await,
            vec!["ringhub.bob[alice]".to_string()]
        );

        // The canonical envelope arrived at ingress with the resolved thread stamped,
        // address intact (no collapse).
        let bytes = rx.recv().await.unwrap();
        let env = decode_envelope(&bytes).unwrap();
        assert_eq!(env.meta.thread, "inst-000001");
        let to = env.meta.to.unwrap();
        assert_eq!(to.organism(), Some("bob"));
        assert_eq!(to.namespace(), Some("ringhub"));
        assert_eq!(to.buffer().map(|s| s.name.as_str()), Some("dm"));
    }

    #[tokio::test]
    async fn reuses_instance_thread() {
        let (tx, mut rx) = mpsc::channel(8);
        let mat = TestMat::default();
        let materialized = mat.materialized.clone();
        let sb = Switchboard::new(mat, tx);

        sb.route(env_to("bob[alice]")).await.unwrap();
        sb.route(env_to("bob[alice].dm")).await.unwrap();

        // Same instance → materialized once, same delivery thread on both injects.
        assert_eq!(materialized.lock().await.len(), 1);
        let t1 = decode_envelope(&rx.recv().await.unwrap()).unwrap().meta.thread;
        let t2 = decode_envelope(&rx.recv().await.unwrap()).unwrap().meta.thread;
        assert_eq!(t1, t2);
    }

    #[tokio::test]
    async fn unknown_organism_errors_and_injects_nothing() {
        let (tx, mut rx) = mpsc::channel(8);
        let sb = Switchboard::new(TestMat::default(), tx);

        let err = sb.route(env_to("nobody[x]")).await.unwrap_err();
        assert!(matches!(err, SwitchboardError::Materialize(_)));
        assert!(rx.try_recv().is_err(), "nothing should be injected on failure");
    }

    #[tokio::test]
    async fn no_destination_errors() {
        let (tx, _rx) = mpsc::channel(8);
        let sb = Switchboard::new(TestMat::default(), tx);

        let env = Envelope {
            meta: Meta {
                from: "ext".into(),
                to: None,
                thread: "t".into(),
                provenance: Provenance::EMPTY,
            },
            payload: Payload::single("Greeting", "text", "hi"),
        };
        assert!(matches!(
            sb.route(env).await,
            Err(SwitchboardError::NoDestination)
        ));
    }
}
