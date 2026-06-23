//! Federation — the cross-node tier (the address namespace IS the node).
//!
//! Each node runs a federation server; nodes mutually register (`namespace ↔ peer`) and
//! exchange canonical envelopes over a symmetrically encrypted link. AgentOS addresses
//! `ringhub.bob` → the federation server sees namespace `ringhub` is a registered peer →
//! seals and transmits it; RingHub symmetrically addresses `agentos.concierge[alice]`.
//!
//! This module owns the **wire protocol** between federation servers — the sealed-frame
//! framing and the peer directory. The transport (TCP/websocket: moving sealed bytes) is
//! a layered hook; the pipeline wiring (egress handler, route-stage namespace check) sits
//! above. The switchboard stays pure-local — it never sees a remote namespace.
//!
//! ## Trust (2026-06-15) — symmetric AEAD, hand-delivered pre-shared keys
//!
//! No auth-negotiation scheme yet. A peer is authenticated **by possession of the shared
//! key**: the AEAD verify gives authentication + integrity for free, so a wrong-key or
//! tampered frame fails to [`open`] (fail-closed). The opened message is still treated as
//! **untrusted ingress** downstream (decode → re-validate → switchboard) — node-auth is
//! not data-trust.
//!
//! ## Node + message authentication are wired in now (cheap seam, hard to retrofit)
//!
//! Even though the current scheme is just pre-shared-key AEAD, the frame carries an
//! **AEAD-authenticated header** so stronger auth can be added without a breaking reframe:
//! - **Node authentication** — the header carries the *sender's node identity*, bound into
//!   the AEAD associated data. [`open`] looks up that sender's key, verifies, and returns
//!   the **authenticated** sender; the claim can't be forged or relabeled. Today the proof
//!   is "holds the shared key"; `auth_method` reserves the upgrade to per-node signatures.
//! - **Message authentication** — the Poly1305 tag (over plaintext *and* header).
//! - `version` + `auth_method` are the retrofit escape hatches.
//!
//! ## Wire protocol (RingHub's Python side implements the identical scheme)
//!
//! ```text
//! header = version:u8 ‖ auth_method:u8 ‖ sender_len:u16(BE) ‖ sender:utf8
//! frame  = header ‖ nonce:24 ‖ XChaCha20Poly1305(key, nonce, encode_envelope(env), aad=header)
//! ```
//! version = 1, auth_method = 0 (pre-shared-key). Plaintext is the canonical envelope via
//! the existing codec — XML for now (cross-language), swappable to a binary cross-language
//! codec with Commit 2.

use std::collections::HashMap;

use std::sync::Arc;

use async_trait::async_trait;
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use tokio::sync::mpsc;

use crate::codec::{decode_envelope, encode_envelope};
use crate::error::PipelineError;
use crate::wire::{Address, Envelope, Provenance, Segment};

/// A 256-bit symmetric key for a peer link, hand-delivered out of band.
pub type PeerKey = [u8; 32];

/// XChaCha20-Poly1305 nonce length.
const NONCE_LEN: usize = 24;
/// Current wire-protocol version.
const VERSION: u8 = 1;
/// Auth method: pre-shared-key AEAD (the only one for now; reserved values upgrade it).
const AUTH_PSK: u8 = 0;

/// Errors from the federation wire layer.
#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    /// No peer registered for the (authenticated) sender namespace.
    #[error("unknown peer namespace: {0}")]
    UnknownPeer(String),
    /// The frame is malformed / shorter than its declared structure.
    #[error("malformed frame")]
    MalformedFrame,
    /// Unsupported protocol version.
    #[error("unsupported federation protocol version: {0}")]
    UnsupportedVersion(u8),
    /// Unsupported auth method.
    #[error("unsupported auth method: {0}")]
    UnsupportedAuth(u8),
    /// Decrypt/authentication failed — wrong key or a tampered frame/header.
    #[error("decrypt/authentication failed (wrong key or tampered frame)")]
    OpenFailed,
    /// Could not gather randomness for the nonce.
    #[error("nonce generation failed")]
    Nonce,
    /// The envelope has no destination address.
    #[error("envelope has no destination address")]
    NoDestination,
    /// The destination's leading segment is not a registered remote node.
    #[error("not a remote node: {0}")]
    NotRemote(String),
    /// The transport failed to send the frame.
    #[error("transport error: {0}")]
    Transport(String),
    /// Local delivery of an inbound envelope failed.
    #[error("local delivery error: {0}")]
    Delivery(String),
    /// The authorizer rejected this origin→target crossing (fail-closed).
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Envelope encode/decode failed.
    #[error(transparent)]
    Codec(#[from] PipelineError),
}

/// Re-root an inbound `from` address onto the authenticated peer `namespace`, overwriting
/// whatever namespace the sender claimed. This is what makes origin **unforgeable**: a peer
/// can claim *which of its agents* sent the message, but not *which namespace* — it can
/// never assert `root` (or any namespace but its own) on the front.
///
/// Convention (federation): the leading segment is the namespace/node. So with ≥2 segments
/// the claimed leading namespace is replaced; a bare single-segment agent is prefixed.
fn reroot_origin(from: &Address, namespace: &str) -> Address {
    let mut segments = vec![Segment {
        name: namespace.to_string(),
        key: None,
    }];
    if from.segments.len() >= 2 {
        segments.extend_from_slice(&from.segments[1..]); // drop the claimed namespace
    } else {
        segments.extend_from_slice(&from.segments); // bare agent → prefix
    }
    Address { segments }
}

/// Authorization at the federation seam: given the **edge-stamped, unforgeable** origin
/// `from` and the target `to`, may this crossing proceed?
///
/// This re-homes the old platform `check_namespace` matrix. The *rule* is the host's (the
/// tenant/namespace matrix — e.g. "a namespaced remote origin may never reach the `root`
/// namespace"); the *seam* — a guaranteed call, post-stamp, pre-delivery, that fails closed
/// on rejection — is rust-pipeline's. Mechanism here, policy in the host.
pub trait Authorizer: Send + Sync {
    fn authorize(&self, from: &Address, to: &Address) -> Result<(), String>;
}

/// An authorizer that permits everything — for local/trusted setups and tests. Federation
/// deployments inject a real matrix instead.
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn authorize(&self, _from: &Address, _to: &Address) -> Result<(), String> {
        Ok(())
    }
}

/// Encode the authenticated header: `version ‖ auth_method ‖ sender_len ‖ sender`.
fn encode_header(sender: &str) -> Vec<u8> {
    let s = sender.as_bytes();
    let mut h = Vec::with_capacity(4 + s.len());
    h.push(VERSION);
    h.push(AUTH_PSK);
    h.extend_from_slice(&(s.len() as u16).to_be_bytes());
    h.extend_from_slice(s);
    h
}

/// Parse the header; returns `(header_bytes, sender, body_offset)`. The header bytes are
/// returned verbatim for use as AEAD associated data (so the sender claim is bound).
fn parse_header(frame: &[u8]) -> Result<(&[u8], String, usize), FederationError> {
    if frame.len() < 4 {
        return Err(FederationError::MalformedFrame);
    }
    if frame[0] != VERSION {
        return Err(FederationError::UnsupportedVersion(frame[0]));
    }
    if frame[1] != AUTH_PSK {
        return Err(FederationError::UnsupportedAuth(frame[1]));
    }
    let sender_len = u16::from_be_bytes([frame[2], frame[3]]) as usize;
    let end = 4 + sender_len;
    if frame.len() < end {
        return Err(FederationError::MalformedFrame);
    }
    let sender = std::str::from_utf8(&frame[4..end])
        .map_err(|_| FederationError::MalformedFrame)?
        .to_string();
    Ok((&frame[..end], sender, end))
}

/// Seal a canonical envelope from `sender` (this node) to a peer holding `key`.
///
/// `frame = header(sender) ‖ nonce ‖ AEAD(key, nonce, encode_envelope(env), aad=header)`.
pub fn seal(envelope: &Envelope, sender: &str, key: &PeerKey) -> Result<Vec<u8>, FederationError> {
    let plaintext = encode_envelope(envelope)?;
    let header = encode_header(sender);
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).map_err(|_| FederationError::OpenFailed)?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce_bytes).map_err(|_| FederationError::Nonce)?;
    let nonce = XNonce::from_slice(&nonce_bytes);

    let ct = cipher
        .encrypt(
            nonce,
            Payload {
                msg: &plaintext,
                aad: &header,
            },
        )
        .map_err(|_| FederationError::OpenFailed)?;

    let mut out = header;
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Open a sealed frame, authenticating the sender against the peer directory.
///
/// Reads the sender from the header, looks up *that peer's* key, and AEAD-verifies (with
/// the header as associated data). Returns the canonical envelope and the **authenticated**
/// sender namespace. Fails closed on unknown sender, wrong key, or any tampering.
pub fn open(frame: &[u8], directory: &PeerDirectory) -> Result<(Envelope, String), FederationError> {
    let (header, sender, body_off) = parse_header(frame)?;

    let peer = directory
        .get(&sender)
        .ok_or_else(|| FederationError::UnknownPeer(sender.clone()))?;

    let body = &frame[body_off..];
    if body.len() < NONCE_LEN {
        return Err(FederationError::MalformedFrame);
    }
    let (nonce_bytes, ct) = body.split_at(NONCE_LEN);

    let cipher =
        XChaCha20Poly1305::new_from_slice(&peer.key).map_err(|_| FederationError::OpenFailed)?;
    let nonce = XNonce::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, Payload { msg: ct, aad: header })
        .map_err(|_| FederationError::OpenFailed)?;

    let envelope = decode_envelope(&plaintext)?;
    Ok((envelope, sender))
}

/// A registered peer node: its namespace (= node id), transport endpoint, and shared key.
#[derive(Clone)]
pub struct Peer {
    pub namespace: String,
    pub endpoint: String,
    pub key: PeerKey,
    /// Provenance unioned onto every envelope opened from this peer — the edge stamp.
    /// **Host-configured** (rust-pipeline stays policy-free about what the bits *mean*);
    /// this is how "came from RingHub" gets marked without rust-pipeline knowing what
    /// RingHub is. Default empty.
    pub inbound_provenance: Provenance,
}

/// The peer directory — `namespace → Peer`. Populated by mutual registration
/// (hand-configured for now). The route stage consults [`PeerDirectory::is_remote`] to
/// decide local-vs-remote before routing.
#[derive(Default)]
pub struct PeerDirectory {
    peers: HashMap<String, Peer>,
}

impl PeerDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a peer.
    pub fn register(&mut self, peer: Peer) {
        self.peers.insert(peer.namespace.clone(), peer);
    }

    /// Look up a peer by namespace.
    pub fn get(&self, namespace: &str) -> Option<&Peer> {
        self.peers.get(namespace)
    }

    /// Whether `namespace` is a registered remote node (⇒ route via federation, not local).
    pub fn is_remote(&self, namespace: &str) -> bool {
        self.peers.contains_key(namespace)
    }
}

/// The federation-egress hook installed in the pipeline route stage.
///
/// When a message's destination leading segment is a registered remote node, the route
/// stage hands the canonical envelope to `tx` (drained by the federation server's send
/// loop) instead of routing it to a local listener. The switchboard never sees it.
#[derive(Clone)]
pub struct FederationEgress {
    /// Shared peer directory — `is_remote(node)` decides local-vs-remote.
    pub directory: Arc<PeerDirectory>,
    /// Envelopes destined for a remote node are sent here.
    pub tx: mpsc::Sender<Envelope>,
}

/// Moves sealed federation frames to a peer's endpoint. The concrete impl (TCP / websocket
/// / HTTP) is a deployment concern; the federation logic stays testable behind this hook.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a sealed frame to `endpoint`.
    async fn send(&self, endpoint: &str, frame: Vec<u8>) -> Result<(), String>;
}

/// Where opened (inbound) envelopes go locally. The [`crate::switchboard::Switchboard`]
/// implements this (delivery = `route`), but a test/alternate sink can too.
#[async_trait]
pub trait LocalDelivery: Send + Sync {
    async fn deliver(&self, envelope: Envelope) -> Result<(), String>;
}

/// The federation server — the node-boundary tier.
///
/// **Outbound:** [`FederationServer::send`] takes a canonical envelope whose leading
/// address segment is a registered peer node, seals it (stamping *this* node's identity),
/// and transmits via the [`Transport`] hook.
///
/// **Inbound:** [`FederationServer::receive`] opens a sealed frame (authenticating the
/// sender), **unions the peer's edge provenance**, strips this node's own namespace prefix
/// so the address is locally routable, and hands it to [`LocalDelivery`] (the switchboard).
///
/// The switchboard stays pure-local; this server is the only component that knows the
/// network exists.
pub struct FederationServer<T: Transport, D: LocalDelivery> {
    node: String,
    directory: PeerDirectory,
    transport: T,
    local: D,
    authorizer: Box<dyn Authorizer>,
}

impl<T: Transport, D: LocalDelivery> FederationServer<T, D> {
    /// Create a federation server for this `node` (its namespace = the sender identity
    /// stamped on outbound frames). `authorizer` gates inbound origin→target crossings —
    /// use [`AllowAll`] for local/trusted setups, a real matrix for federation.
    pub fn new(
        node: impl Into<String>,
        directory: PeerDirectory,
        transport: T,
        local: D,
        authorizer: impl Authorizer + 'static,
    ) -> Self {
        Self {
            node: node.into(),
            directory,
            transport,
            local,
            authorizer: Box::new(authorizer),
        }
    }

    /// The peer directory (for the route-stage local-vs-remote check).
    pub fn directory(&self) -> &PeerDirectory {
        &self.directory
    }

    /// Outbound: seal a canonical envelope to its destination node and transmit.
    ///
    /// The destination node is the envelope's **leading address segment** (`ringhub.bob`
    /// → node `ringhub`). Fails if that segment isn't a registered peer.
    pub async fn send(&self, envelope: Envelope) -> Result<(), FederationError> {
        let to = envelope
            .meta
            .to
            .as_ref()
            .ok_or(FederationError::NoDestination)?;
        let node = to
            .segments
            .first()
            .map(|s| s.name.as_str())
            .ok_or(FederationError::NoDestination)?;
        let peer = self
            .directory
            .get(node)
            .ok_or_else(|| FederationError::NotRemote(node.to_string()))?;

        let frame = seal(&envelope, &self.node, &peer.key)?;
        let endpoint = peer.endpoint.clone();
        self.transport
            .send(&endpoint, frame)
            .await
            .map_err(FederationError::Transport)?;
        Ok(())
    }

    /// Inbound: open a peer frame, stamp the edge provenance, strip our own namespace, and
    /// deliver locally.
    pub async fn receive(&self, frame: &[u8]) -> Result<(), FederationError> {
        let (mut envelope, sender) = open(frame, &self.directory)?;

        // UNFORGEABLE ORIGIN: stamp `from`'s namespace from the authenticated peer
        // identity, overwriting any namespace the sender claimed. A peer can name *which
        // of its agents* sent this, never *which namespace* — `root` is structurally
        // unstampable by a remote edge. (Sibling of the provenance stamp below.)
        envelope.meta.from = reroot_origin(&envelope.meta.from, &sender);

        // Edge provenance stamp (host-configured per peer).
        let edge = self
            .directory
            .get(&sender)
            .map(|p| p.inbound_provenance)
            .unwrap_or(Provenance::EMPTY);
        envelope.meta.provenance.union_with(edge);

        // AUTHORIZE: from × to, against the host's matrix (re-homed check_namespace). `from`
        // is now the unforgeable stamped origin; `to` still carries its namespace. Fail-closed.
        if let Some(to) = envelope.meta.to.as_ref() {
            self.authorizer
                .authorize(&envelope.meta.from, to)
                .map_err(FederationError::Unauthorized)?;
        }

        // Strip our own node prefix so the switchboard sees a local address:
        // `agentos.concierge[alice]` received at node `agentos` → `concierge[alice]`.
        if let Some(to) = envelope.meta.to.as_mut() {
            if to.segments.len() > 1 && to.segments[0].name == self.node {
                to.segments.remove(0);
            }
        }

        self.local
            .deliver(envelope)
            .await
            .map_err(FederationError::Delivery)?;
        Ok(())
    }

    /// Drain a federation-egress receiver, transmitting each envelope to its peer node.
    /// Spawn this in a task and pair its `tx` with [`FederationEgress`]. Per-message errors
    /// are logged and skipped (one bad destination doesn't stop the link).
    pub async fn run_egress(&self, mut rx: mpsc::Receiver<Envelope>) {
        while let Some(envelope) = rx.recv().await {
            if let Err(e) = self.send(envelope).await {
                tracing::warn!("federation egress send failed: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Address, Meta, Payload as WirePayload, Provenance};

    fn sample() -> Envelope {
        Envelope {
            meta: Meta {
                from: Address::parse("agentos.concierge[alice]").unwrap(),
                to: Some(Address::parse("ringhub.bob").unwrap()),
                thread: "t-fed".into(),
                provenance: Provenance::from_bit(3),
            },
            payload: WirePayload::single("Order", "item", "two coffees"),
        }
    }

    /// A directory where peer `sender` is reachable with `key`.
    fn dir_with(sender: &str, key: PeerKey) -> PeerDirectory {
        let mut d = PeerDirectory::new();
        d.register(Peer {
            namespace: sender.into(),
            endpoint: "test".into(),
            key,
            inbound_provenance: Provenance::EMPTY,
        });
        d
    }

    #[test]
    fn seal_open_round_trip_authenticates_sender() {
        let key: PeerKey = [7u8; 32];
        let env = sample();
        let frame = seal(&env, "agentos", &key).unwrap();
        // payload not in clear
        assert!(!frame.windows(7).any(|w| w == b"coffees"));

        let (back, sender) = open(&frame, &dir_with("agentos", key)).unwrap();
        assert_eq!(back, env);
        assert_eq!(sender, "agentos"); // authenticated node identity
    }

    #[test]
    fn wrong_key_fails_closed() {
        let frame = seal(&sample(), "agentos", &[1u8; 32]).unwrap();
        let err = open(&frame, &dir_with("agentos", [2u8; 32])).unwrap_err();
        assert!(matches!(err, FederationError::OpenFailed));
    }

    #[test]
    fn tampered_ciphertext_fails_closed() {
        let key: PeerKey = [9u8; 32];
        let mut frame = seal(&sample(), "agentos", &key).unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 0xff;
        assert!(matches!(
            open(&frame, &dir_with("agentos", key)),
            Err(FederationError::OpenFailed)
        ));
    }

    #[test]
    fn tampered_sender_claim_rejected() {
        // The sender identity is AEAD-bound: flipping a sender byte either points at an
        // unregistered peer or breaks the AAD — either way, rejected.
        let key: PeerKey = [5u8; 32];
        let mut frame = seal(&sample(), "agentos", &key).unwrap();
        frame[4] ^= 0x01; // first byte of the sender name
        assert!(open(&frame, &dir_with("agentos", key)).is_err());
    }

    #[test]
    fn unknown_sender_rejected() {
        let frame = seal(&sample(), "agentos", &[4u8; 32]).unwrap();
        let err = open(&frame, &dir_with("someone-else", [4u8; 32])).unwrap_err();
        assert!(matches!(err, FederationError::UnknownPeer(_)));
    }

    #[test]
    fn bad_version_rejected() {
        let key: PeerKey = [6u8; 32];
        let mut frame = seal(&sample(), "agentos", &key).unwrap();
        frame[0] = 2; // bump version
        assert!(matches!(
            open(&frame, &dir_with("agentos", key)),
            Err(FederationError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn distinct_nonces_per_seal() {
        let key: PeerKey = [3u8; 32];
        let env = sample();
        let a = seal(&env, "agentos", &key).unwrap();
        let b = seal(&env, "agentos", &key).unwrap();
        assert_ne!(a, b);
        let dir = dir_with("agentos", key);
        assert_eq!(open(&a, &dir).unwrap().0, open(&b, &dir).unwrap().0);
    }

    #[test]
    fn peer_directory_routing() {
        let mut dir = PeerDirectory::new();
        dir.register(Peer {
            namespace: "ringhub".into(),
            endpoint: "https://ringhub.local/fed".into(),
            key: [0u8; 32],
            inbound_provenance: Provenance::EMPTY,
        });
        assert!(dir.is_remote("ringhub"));
        assert!(!dir.is_remote("agentos")); // local node → not in peer directory
        assert_eq!(dir.get("ringhub").unwrap().endpoint, "https://ringhub.local/fed");
    }

    // ── FederationServer ──

    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingTransport {
        sent: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
    }
    #[async_trait]
    impl Transport for RecordingTransport {
        async fn send(&self, endpoint: &str, frame: Vec<u8>) -> Result<(), String> {
            self.sent.lock().await.push((endpoint.to_string(), frame));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        delivered: Arc<Mutex<Vec<Envelope>>>,
    }
    #[async_trait]
    impl LocalDelivery for RecordingSink {
        async fn deliver(&self, envelope: Envelope) -> Result<(), String> {
            self.delivered.lock().await.push(envelope);
            Ok(())
        }
    }

    fn peer(ns: &str, key: PeerKey, inbound_bit: u8) -> Peer {
        Peer {
            namespace: ns.into(),
            endpoint: format!("https://{ns}.local/fed"),
            key,
            inbound_provenance: Provenance::from_bit(inbound_bit),
        }
    }

    #[tokio::test]
    async fn send_seals_to_the_right_peer() {
        let key: PeerKey = [11u8; 32];
        let transport = RecordingTransport::default();
        let sent = transport.sent.clone();

        let mut dir = PeerDirectory::new();
        dir.register(peer("ringhub", key, 10));
        let server =
            FederationServer::new("agentos", dir, transport, RecordingSink::default(), AllowAll);

        // Address a message to ringhub.bob[alice] — leading segment "ringhub" is the node.
        let env = Envelope {
            meta: Meta {
                from: Address::flat("concierge"),
                to: Some(Address::parse("ringhub.bob[alice]").unwrap()),
                thread: "t".into(),
                provenance: Provenance::EMPTY,
            },
            payload: WirePayload::single("Order", "item", "coffee"),
        };
        server.send(env.clone()).await.unwrap();

        let sent = sent.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "https://ringhub.local/fed"); // right endpoint
        // The captured frame opens (from agentos's perspective on the receiving side).
        let (opened, sender) = open(&sent[0].1, &dir_with("agentos", key)).unwrap();
        assert_eq!(sender, "agentos");
        assert_eq!(opened, env);
    }

    #[tokio::test]
    async fn send_rejects_non_peer_destination() {
        let server = FederationServer::new(
            "agentos",
            PeerDirectory::new(),
            RecordingTransport::default(),
            RecordingSink::default(),
            AllowAll,
        );
        let env = Envelope {
            meta: Meta {
                from: Address::flat("x"),
                to: Some(Address::parse("bob[alice]").unwrap()), // local, not a peer node
                thread: "t".into(),
                provenance: Provenance::EMPTY,
            },
            payload: WirePayload::single("Order", "item", "coffee"),
        };
        assert!(matches!(
            server.send(env).await,
            Err(FederationError::NotRemote(_))
        ));
    }

    #[tokio::test]
    async fn receive_stamps_edge_provenance_and_strips_self_namespace() {
        let key: PeerKey = [22u8; 32];

        // A frame from ringhub → agentos, addressed to agentos.concierge[alice].
        let inbound = Envelope {
            meta: Meta {
                from: Address::parse("ringhub.bob").unwrap(),
                to: Some(Address::parse("agentos.concierge[alice]").unwrap()),
                thread: "t".into(),
                provenance: Provenance::from_bit(2), // some pre-existing label
            },
            payload: WirePayload::single("Reply", "text", "ok"),
        };
        let frame = seal(&inbound, "ringhub", &key).unwrap();

        // agentos's federation server: peer "ringhub" stamps edge bit 10 on inbound.
        let mut dir = PeerDirectory::new();
        dir.register(peer("ringhub", key, 10));
        let sink = RecordingSink::default();
        let delivered = sink.delivered.clone();
        let server =
            FederationServer::new("agentos", dir, RecordingTransport::default(), sink, AllowAll);

        server.receive(&frame).await.unwrap();

        let delivered = delivered.lock().await;
        assert_eq!(delivered.len(), 1);
        let env = &delivered[0];
        // Origin re-rooted to the authenticated peer namespace (here unchanged).
        assert_eq!(env.meta.from.to_string(), "ringhub.bob");
        // Self-namespace stripped → locally routable.
        assert_eq!(env.meta.to.as_ref().unwrap().to_string(), "concierge[alice]");
        assert_eq!(env.meta.to.as_ref().unwrap().organism(), Some("concierge"));
        // Edge provenance unioned in, original preserved.
        assert!(env.meta.provenance.contains_bit(10)); // edge stamp
        assert!(env.meta.provenance.contains_bit(2)); // carried
    }

    #[tokio::test]
    async fn receive_rejects_unknown_peer() {
        let frame = seal(&sample(), "ringhub", &[1u8; 32]).unwrap();
        let server = FederationServer::new(
            "agentos",
            PeerDirectory::new(), // ringhub not registered
            RecordingTransport::default(),
            RecordingSink::default(),
            AllowAll,
        );
        assert!(matches!(
            server.receive(&frame).await,
            Err(FederationError::UnknownPeer(_))
        ));
    }

    #[tokio::test]
    async fn receive_reroots_forged_origin_namespace() {
        // A compromised peer forges `from: root.evil` to claim admin authority.
        let key: PeerKey = [44u8; 32];
        let forged = Envelope {
            meta: Meta {
                from: Address::parse("root.evil").unwrap(), // lie
                to: Some(Address::parse("agentos.concierge[alice]").unwrap()),
                thread: "t".into(),
                provenance: Provenance::EMPTY,
            },
            payload: WirePayload::single("X", "a", "b"),
        };
        let frame = seal(&forged, "ringhub", &key).unwrap();

        let mut dir = PeerDirectory::new();
        dir.register(peer("ringhub", key, 0));
        let sink = RecordingSink::default();
        let delivered = sink.delivered.clone();
        let server =
            FederationServer::new("agentos", dir, RecordingTransport::default(), sink, AllowAll);

        server.receive(&frame).await.unwrap();

        // The forged `root` namespace is overwritten with the authenticated peer's.
        let env = &delivered.lock().await[0];
        assert_eq!(env.meta.from.to_string(), "ringhub.evil");
        assert_eq!(env.meta.from.node(), Some("ringhub"));
        assert_ne!(env.meta.from.node(), Some("root"));
    }

    #[tokio::test]
    async fn receive_blocked_by_authorizer_delivers_nothing() {
        // A matrix that forbids the `ringhub` origin from reaching the `root` namespace.
        struct NoRinghubToRoot;
        impl Authorizer for NoRinghubToRoot {
            fn authorize(&self, from: &Address, to: &Address) -> Result<(), String> {
                // Federation authz keys on the node (leading segment), not namespace().
                if from.node() == Some("ringhub") && to.node() == Some("root") {
                    Err("ringhub may not reach root".into())
                } else {
                    Ok(())
                }
            }
        }

        let key: PeerKey = [55u8; 32];
        // ringhub tries to reach root.coding-expert (escalation).
        let env = Envelope {
            meta: Meta {
                from: Address::parse("ringhub.cart").unwrap(),
                to: Some(Address::parse("root.coding-expert[x]").unwrap()),
                thread: "t".into(),
                provenance: Provenance::EMPTY,
            },
            payload: WirePayload::single("X", "a", "b"),
        };
        let frame = seal(&env, "ringhub", &key).unwrap();

        let mut dir = PeerDirectory::new();
        dir.register(peer("ringhub", key, 0));
        let sink = RecordingSink::default();
        let delivered = sink.delivered.clone();
        let server = FederationServer::new(
            "agentos",
            dir,
            RecordingTransport::default(),
            sink,
            NoRinghubToRoot,
        );

        let err = server.receive(&frame).await.unwrap_err();
        assert!(matches!(err, FederationError::Unauthorized(_)));
        assert!(delivered.lock().await.is_empty(), "blocked → nothing delivered");
    }
}
