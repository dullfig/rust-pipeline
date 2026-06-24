# rust_pipeline_federation (Python)

The Python **mirror** of rust-pipeline's federation wire protocol — the byte-compatible
counterpart to `src/federation.rs` + `src/codec.rs`.

> **INTERNAL.** This is a dependency of **`agentos-client`** (agentos-owned; it provides the
> consumer-facing Bob-client API). It is **not for direct consumer use** — RingHub depends on
> `agentos-client`, never on this module, and never vendors it. The protocol layer stays
> protocol-only (no host helpers / config / client API here).
> Boundary owned by the integration seam: `ringhub-integration/PYTHON_CLIENT_UNTANGLE.md`.

It is **byte-for-byte interoperable** with the Rust side, verified by the cross-language tests
below (seal in one language, open in the other, both directions). That byte-compatibility is
the contract — if the Rust side changes the frame or envelope format, update this module and
re-run the tests.

## Install (for `agentos-client` only)

`agentos-client` depends on this module; **consumers never install it directly.** RingHub
depends on `agentos-client` and never references rust-pipeline — no import, no vendored copy,
no `git+…rust-pipeline` reference (INV-1, INV-3, INV-4).

For `agentos-client`'s build to pull this protocol mirror (agentos chooses the exact channel —
a git ref, or a wheel on a private index):
```
pip install "git+https://github.com/dullfig/rust-pipeline.git#subdirectory=python"
# or build a wheel for a private index:  cd python && python -m build
```

**Don't publish to public PyPI** — it's internal. (`pynacl` is the one runtime dependency.)

## Verify

- `python selftest.py` — **no Rust needed.** Python round-trip + opens a frozen *real*
  Rust-sealed frame + wrong-key/tamper rejection. Run it in CI (rust-pipeline + agentos-client).
- `python interop_test.py` — full cross-language check (builds the Rust `fed_interop`
  example via cargo; requires the rust-pipeline crate alongside). Seals in each language,
  opens in the other.

## Usage

```python
import rust_pipeline_federation as fed

# Peer directory: each peer is namespace ↔ endpoint ↔ pre-shared 32-byte key.
directory = fed.PeerDirectory()
directory.register(fed.Peer("agentos", "https://agentos.local/fed", AGENTOS_KEY,
                            inbound_provenance=0))

# Your transport (move sealed bytes) and your local handler (process an opened envelope).
def transport_send(endpoint, frame):  ...   # POST frame to endpoint, or a websocket, etc.
def local_deliver(envelope):          ...   # hand to Django

# Optional authorize(from, to) -> bool is your `from x to` namespace matrix
# (re-homes check_namespace). Keys on Address.node() (leading segment). None = allow all.
def authorize(frm, to):
    return not (frm.node() != "ringhub" and to.node() == "root")  # e.g. nobody reaches root

server = fed.FederationServer("ringhub", directory, transport_send, local_deliver, authorize)

# Outbound: address an AgentOS agent; the leading segment is the node.
env = fed.Envelope(
    fed.Meta(from_=fed.Address.parse("ringhub.cart"),
             to=fed.Address.parse("agentos.concierge[alice]"),
             thread="order-42", provenance=0),
    fed.Payload.single("Order", "item", "two coffees"),
)
server.send(env)            # seals with RingHub's identity + AgentOS's key, transmits

# Inbound: when a sealed frame arrives over your transport:
server.receive(frame_bytes)  # authenticates sender, RE-ROOTS `from` to the authenticated
                             # peer (unforgeable origin), stamps edge provenance, runs
                             # authorize (fail-closed), strips self-prefix, calls local_deliver
```

Build richer payloads with `fed.PayloadValue` (`rec`, `seq`, `text`, `uint`, `sint`,
`real`, `boolean`, `blob`, `nil`); read them with `value.get("field")` / `.as_text()`.

## Wire protocol

```
header = version:u8(=1) ‖ auth_method:u8(=0 pre-shared-key) ‖ sender_len:u16(BE) ‖ sender:utf8
frame  = header ‖ nonce:24 ‖ XChaCha20Poly1305(key, nonce, encode_envelope(env), aad=header)
```

- **Crypto:** XChaCha20-Poly1305 IETF (libsodium via PyNaCl == RustCrypto). 256-bit
  hand-delivered pre-shared per-peer key. No auth-negotiation scheme yet.
- **Node auth:** the sender's node identity is in the header, bound as AEAD associated
  data; `open_frame` looks up that sender's key, verifies, and returns the *authenticated*
  sender. The claim can't be forged or relabeled. `version`/`auth_method` reserve upgrades
  (e.g. ed25519 per-node signatures) without a breaking reframe.
- **Plaintext:** the canonical envelope as XML (cross-language; XML for now — swappable to
  a binary cross-language codec later, in lockstep with the Rust side).
- **Fail-closed:** unknown sender, wrong key, or any tampering raises `ValueError`.
- **Inbound is still untrusted ingress** on the AgentOS side (re-validated downstream);
  node-auth is not data-trust, so data provenance is stamped separately.
