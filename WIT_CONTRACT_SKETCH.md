# Commit 1 — WIT Contract + Handler-Contract Sketch

**Status:** Design sketch for review. 2026-06-14. Pairs with `WIRE_REDESIGN.md` (Commit 1, green-lit).
**Covers the two halves of Commit 1:** (A) the canonical envelope as a WIT type, and (B) the *encapsulated* handler contract that stops agentos touching raw bytes. Half B is where the real tension is — it changes the `Handler` trait every tool implements.

Not implemented. WIT is illustrative (syntax approximate); the point is the shape and the decisions.

---

## A. The WIT envelope type

```wit
package rust-pipeline:wire@0.1.0;

/// The ONE canonical envelope — shared by intra-instance hops and inter-instance
/// (cross-pipeline) delivery. There is no second envelope. Kills the double-
/// enveloping at agentos runtime_impl.rs:151.
interface wire {
    /// One segment of a hierarchical address: a name + optional instance key +
    /// optional cache-composition keys. "ringhub.bob[alice].calendar" -> 3 segments.
    /// A bare "greeter" -> 1 segment, no key: the DEGENERATE FLAT CASE that replaces
    /// today's `from: String` / `to: String`.
    record segment {
        name: string,
        key: option<string>,
        cache-keys: list<string>,
    }

    /// Hierarchical agent address. Flat single-listener routing is a one-segment
    /// address with no key — same type, no special case. This is the routing key
    /// for BOTH a local thread hop and a cross-pipeline hand-off (see §C).
    record address {
        segments: list<segment>,
    }

    /// Opaque provenance label-set. SET semantics: union = bitwise OR, egress test
    /// = mask AND. rust-pipeline CARRIES and UNIONS; it NEVER interprets a bit.
    /// agentos owns the bit->meaning map (operation catalog, destination allowlist)
    /// entirely out of band.
    ///
    /// Deliberately a neutral bitset, NOT WIT `flags`: `flags` forces naming its
    /// members, which would drag policy (external-web, member-*) INTO rust-pipeline
    /// and break "carry it, don't interpret it." agentos may define its own WIT
    /// `flags` that maps onto these bits. 128 bits; widen to list<u64> if outgrown.
    record provenance {
        bits-lo: u64,
        bits-hi: u64,
    }

    /// Trust-relevant header.
    record meta {
        /// Last hop. ROUTING/AUDIT ONLY — no enforcement predicate may read this.
        from: address,
        /// Destination; none = unrouted/broadcast.
        to: option<address>,
        /// Opaque conversation/thread id (UUID string today).
        thread: string,
        /// Durable origin set; rust-pipeline unions this into every envelope it
        /// builds downstream. The handler never writes it (§B).
        provenance: provenance,
    }

    /// A typed, tagged payload. `tag` selects the operation/schema; `value` is the
    /// DECODED, schema-validated content — never raw wire bytes at the handler
    /// boundary (§B). The wire encoding of `value` (XML now, binary later) is
    /// internal to rust-pipeline and invisible to agentos.
    record payload {
        tag: string,
        value: payload-value,
    }

    /// Self-describing, format-agnostic decoded value. Lets the pipeline stay
    /// generic over per-operation payload schemas while keeping XML/codec details
    /// out of agentos. Handlers decode this into their own concrete type (§B).
    variant payload-value {
        rec(list<field>),
        seq(list<payload-value>),
        text(string),
        uint(u64),
        sint(s64),
        real(f64),
        boolean(bool),
        blob(list<u8>),
        nil,
    }
    record field { name: string, value: payload-value }

    /// The one canonical envelope.
    record envelope { meta: meta, payload: payload }
}
```

---

## B. The encapsulated handler contract (the load-bearing change)

**Today the wire format leaks into agentos** at three spots — these are what "agentos
works with the typed value, not raw bytes" must eliminate:
- `ValidatedPayload { xml: Vec<u8>, tag }` — handlers parse XML themselves.
- `HandlerResponse::Send { payload_xml: Vec<u8> }` / `Reply { payload_xml }` — handlers serialize XML themselves.
- platform `Envelope.body: Vec<u8>` — opaque wire bytes across instances.

### Before → After (Rust)

```rust
// ── BEFORE (wire leaks out) ──
pub struct ValidatedPayload { pub xml: Vec<u8>, pub tag: String }
pub enum HandlerResponse {
    Send  { to: AgentId, payload_xml: Vec<u8> },
    Reply { payload_xml: Vec<u8> },
    None,
}
pub struct HandlerContext { pub thread_id: ThreadId, pub from: AgentId, pub own_name: AgentId }

// ── AFTER (encapsulated: typed values only; rust-pipeline owns bytes) ──
/// Decoded, schema-validated payload. No bytes, no XML, no codec.
pub struct ValidatedPayload { pub tag: String, pub value: PayloadValue }
impl ValidatedPayload {
    /// Decode into a concrete (wit-bindgen / serde) payload type.
    pub fn decode<T: FromPayload>(&self) -> Result<T, DecodeError> { /* ... */ }
}

pub enum HandlerResponse {
    Send  { to: Address, payload: OutgoingPayload },
    Reply { payload: OutgoingPayload },
    None,
    // RESERVED (§3.3 batch-scatter, NOT Commit 1). Shape left open so adding it
    // later is additive, not a contract break:
    //   Scatter { sends: Vec<(Address, OutgoingPayload)> },
}

/// An outgoing payload built by tag + typed value. rust-pipeline encodes it at the
/// build seam; the handler NEVER serializes and never sees the codec.
pub struct OutgoingPayload { pub tag: String, pub value: PayloadValue }
impl OutgoingPayload {
    pub fn new<T: ToPayload>(tag: &str, value: &T) -> Self { /* ... */ }
}

pub struct HandlerContext {
    pub thread_id: ThreadId,
    pub from: Address,        // was AgentId/String
    pub own_name: Address,
    // NB: no writable provenance here. Carry-don't-interpret (§ provenance below).
}
```

### The `PayloadValue` decision (the one real fork)

The pipeline is generic — it can't hold a Rust type per operation. Three ways to give
handlers a *typed* (not-XML) payload:

1. **Self-describing value** *(recommended)* — `PayloadValue` is the decoded tree above;
   handlers `payload.decode::<Greeting>()?` into their own wit-bindgen/serde type. Pipeline
   stays generic and **codec-agnostic**, so the XML→binary swap (Commit 2) is invisible to
   handlers. One typed `decode` step replaces hand-rolled XML parsing.
2. **Canonical bytes + bindgen** — hand the handler canonical bytes to decode. *Rejected:* the
   handler still touches bytes and the bytes are codec-specific, so the binary swap leaks into
   agentos. Breaks encapsulation — the whole point.
3. **Generic `Pipeline<P>`** — one strongly-typed payload `P` per pipeline. *Rejected:* agentos
   listeners are heterogeneous (every tool a different payload); doesn't fit.

**Recommendation: option 1.** It is the only one where Commit 2's codec swap touches zero
agentos code — which is the entire reason encapsulation is a Commit 1 requirement.

### Provenance handling in Commit 1 (carrier only)

- The handler **does not set or read** provenance. At the build seam, rust-pipeline unions
  `inbound.meta.provenance` into the outbound envelope automatically — so a labeled input
  yields a labeled output through any chain, with **no handler cooperation**. That's the whole
  carrier.
- `Send`/`Reply` carry no provenance field; the pipeline supplies it.
- **Reserved (NOT Commit 1):** a source-edge *stamp* tool (gap-report A4) will eventually need
  a channel to set a bit on its output. That's the "extend the handler return channel vs.
  per-listener static label" fork — left open here, built when agentos does provenance policy.
  Commit 1 ships propagation only (1.2 trap: do not report "provenance done").

---

## C. The encode/decode seam (where the bytes live)

The existing functions become internal and typed:

| Today | After | Notes |
|---|---|---|
| `parse_envelope(&[u8]) -> Envelope` | `decode_envelope(&[u8]) -> Envelope` | internal; XML now, binary later; yields typed meta+provenance |
| `build_envelope(from,to,thread,payload_xml) -> Vec<u8>` | `encode_envelope(&Envelope) -> Vec<u8>` | internal; pipeline builds `Envelope` from the handler's typed `HandlerResponse`, unions provenance, then encodes |

Dispatch loop (encapsulated): **ingress bytes → `decode_envelope` → validate `payload.value`
against the schema for `tag` (generated from WIT via `crates/wit`) → hand typed
`ValidatedPayload` to handler → handler returns typed `HandlerResponse` → pipeline builds
outbound `Envelope` (own `from`, target `to`, **unioned provenance**) → `encode_envelope` →
re-inject.**

**Routing — intra vs inter pipeline (ties to the multi-pipeline question):** the same
`address` routes both. If `to` resolves to a *local* listener, it's a thread hop within this
pipeline. If it resolves to *another instance*, the switchboard hands the envelope to that
pipeline's ingress — **by value in-process (no serialization), or `encode_envelope` →
transport → `decode_envelope` cross-process.** One envelope, one address space, one trust
boundary across both. The switchboard's directory (`address → ingress`) + the materialization
hook live behind the existing `Runtime` trait.

---

## D. Open design questions for review

> **RESOLVED (integration-claude, 2026-06-14):**
> - **D.1 `PayloadValue` shape → self-describing value (settled).** Matches dynamic/WASM-loaded tools the pipeline doesn't know at compile time; payloads validated against registered schemas (keyed by `tag`), not statically typed. *Commit 2 interaction to bank:* a self-describing value pre-constrains the binary codec toward self-describing formats (cbor/msgpack) **or** schema-at-decode. Mitigated already — `ValidatedPayload { tag, value }` carries `tag` and the `SchemaRegistry` holds schemas by `tag`, so schema-at-decode is available; **flag this at the codec decision so postcard-style schema-driven codecs aren't ruled in/out by accident.**
> - **D.3 codegen home → Option A (settled, on the merits).** rust-pipeline owns `wire.wit` + standard wit-bindgen (WIT→Rust) + a serde codec. NOT a "for Commit 1" compromise: the envelope is rust-pipeline's transport *content* (needs only off-the-shelf wit-bindgen); agentos's `crates/wit` is a tool-schema *generator* (XML PayloadSchema + LLM JSON Schema + decode grammar) the envelope never uses — sharing it (B) would be a false consolidation. One envelope def (`wire.wit`), clean dependency direction (agentos → rust-pipeline, never reverse), no drift. The generators do not converge; the only future cross-repo coordination is the Commit 2 codec choice — a conversation, not a crate merge.

### Remaining open

1. **`PayloadValue` shape** — is the self-describing variant (D.1) the right generic, or do we
   want `decode<T>` backed by wit-bindgen codegen per payload type? (Affects how "typed" the
   handler experience is.)
2. **Provenance width** — 128-bit record vs `list<u64>`. 128 is simpler; how many distinct
   origin labels does agentos foresee?
3. **Does the switchboard ship in rust-pipeline now, or stay in agentos behind `Runtime`** for
   Commit 1, promoted later? (The envelope is Commit 1 regardless; the fabric is separable.)
4. **`thread` as `string`** vs a typed id — keep opaque for now?
5. **Address parsing** — port agentos's `address.rs` grammar (`name[key+key].buffer`) into
   rust-pipeline as the canonical parser, or keep addresses pre-parsed across the wire?
```
