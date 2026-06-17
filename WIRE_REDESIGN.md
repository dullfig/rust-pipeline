# Wire / Envelope Redesign — Design Decisions

**Status:** GREEN-LIT (integration-claude, 2026-06-14) — build Commit 1. Goal is **XML elimination** (Daniel's call), done **staged + encapsulated**. Convergent direction; code audit de-risked it; build starts now.
**Scope:** rust-pipeline (canonical envelope owner) + the agentos changes it forces.
**One-line:** "XML wire", "two envelopes", "provenance", and "fan-out" are not four problems — they are one WIT type-design. Eliminate XML, but **stage** it (provenance ships first, on the WIT→XML serialization that already exists for free; a binary codec deletes XML as a fast-follow) and **encapsulate** it (agentos works the typed envelope value, never raw bytes — so the wire format is rust-pipeline's to change without a second agentos migration).

---

## 0. Why this note exists

rust-pipeline is the Rust reimplementation of xml-pipeline (Python). The wire was XML for
security-theater reasons (XSD validation as a trust ritual) that have since evaporated —
XML is now **inertia, not a requirement**. The goal is to **eliminate XML** and make **WIT**
(component-model interface types) the canonical source-of-truth type for the envelope. Before
that lands, four findings converged that say the migration should not be done in isolation.

All four touch the *envelope and the handler contract*. Doing them piecemeal — especially
doing any of them in XML first and re-deriving them under WIT later — means designing the
envelope twice and migrating agentos twice. This note records the decisions so the cut-over
carries all four at once.

**XML elimination, made safe by three riders (integration-claude, 2026-06-14):**
1. **Encapsulate serialization in rust-pipeline** (§3.1). agentos works with the **typed WIT envelope value**, never raw bytes. The wire format becomes internal and transparent to agentos — so it migrates *once*, and the format is rust-pipeline's to change freely.
2. **Stage in two commits, don't bundle** (§3.2). Commit 1 = typed envelope + provenance-flags on the WIT→XML serialization `crates/wit` already generates for free. Commit 2 = a binary codec that *deletes* the XML step. Provenance is the valuable/gating part and must not wait on the binary sub-decision.
3. **The binary format is an unscoped sub-decision** (§3.2). "WIT on the wire" ≠ literal wasmtime Canonical ABI (CABI is for component-call lower/lift, awkward as a standalone wire codec). The real choice is which binary format over WIT-derived serde types — postcard / bincode / msgpack / cbor — scoped as the *first task of Commit 2*.

---

## 1. Findings (grounded in current code)

### 1.1 There are two envelopes, and the seam between them drops fields
- **Pipeline (intra-instance) envelope:** `Envelope { meta: Meta { from, to, thread }, payload_raw, payload_tag }` — `rust-pipeline/src/envelope.rs:41-56`. Crosses *hops within* one instance.
- **Platform (inter-instance) envelope:** `Envelope { to: Address, from, body, buffer }` — `agentos/crates/platform/src/router.rs:34-44`. Crosses *between* instances. `body` is opaque bytes that **wrap** a pipeline envelope.
- **The bridge:** `agentos/crates/pipeline/src/runtime_impl.rs:151` calls `build_envelope(from, to, thread_id, &envelope.body)` — double-enveloping, and the hierarchical `Address` (`ringhub.bob[alice].calendar`) is **collapsed to a flat listener name** here.
- agentos runs **one pipeline per instance** (materialization-on-routing) for real reasons: per-instance kernel state, KV shards, lifecycle/eviction, independent backpressure. The thread mechanism separates *conversations*, not *agents-as-resource-domains* — so per-instance pipelines are defensible. The mistake is not "separate pipelines"; it is **inventing a second envelope to glue them**, which fragments the canonical wire format.

### 1.2 Provenance must live partly here, and is inert until tools enforce it
- agentos's gap analysis (`PROTOCOL_CODE_GAPS.md`, `PROTOCOL_PROVENANCE_GAP_REPORT.md`) wants a set-typed **provenance** field on the envelope — explicitly **WIT `flags`, not an enum** (an enum re-introduces the last-hop bug at the type level).
- The **carrier** (field + serialize + union at the `build_envelope` seam) belongs in rust-pipeline: it owns the canonical envelope and the seam (`pipeline.rs:451, 484, 529`).
- The **policy** (stamp tools, egress check, declassifier, catalog/allowlist) belongs in agentos (tools, organism YAML, kernel — none exist in rust-pipeline).
- **Trap to avoid:** the carrier propagates but stops nothing until a source-edge tool stamps AND an egress tool checks. Shipping the carrier alone manufactures a wall that isn't there. rust-pipeline ships the carrier *as a prerequisite*, not as "provenance done."
- The single most-flagged "provenance gets dropped here" seam is exactly the two-envelope bridge (1.1). **Unify the envelope and that failure mode disappears by construction.**

### 1.3 Agent multi-tool fan-out is no longer a pipeline primitive
- `HandlerResponse` is single-valued: `Send { to, payload_xml } | Reply | None` (`rust-pipeline/src/handler.rs:48-69`). The pipeline routes at most one message per handler invocation. No fan-out variant.
- The LLM still emits N tool calls per turn (`pending: Vec<PendingToolCall>`, `agentos/crates/agent/src/handler.rs:382`), but they execute **serially**: `AwaitingTools { pending, collected, current_index }` fires one, waits for its `<ToolResponse>`, advances `current_index`, fires the next (`handler.rs:650-680`).
- **Implication for WIT:** the feature does *not* depend on the XML wire — the pipeline never routes a multi-call document. WIT threatens nothing here.

### 1.4 The buffer is the scatter-gather primitive — parallel-capable, but fed serially
- `BufferHandler` = "fork()+exec() for callable organisms" (`agentos/crates/pipeline/src/buffer.rs:1`): per call, spawn an ephemeral child pipeline from organism YAML, run one task, return a `ToolResponse`.
- Concurrency lives in a semaphore: `Semaphore::new(config.max_concurrency)` (default 5) at `buffer.rs:60,78`. That is the fan-out.
- **But:** it's **one task per call** (`buffer.rs:268-280`), and `handle` awaits the child to completion. Fed by the agent's *serial* `AwaitingTools` loop (1.3), a single agent turn cannot saturate `max_concurrency` — the semaphore is only saturated by multiple agents. "Ten emails at once" is the design intent in the semaphore, **not** the wired behavior.
- **Design direction:** parallelism belongs in the buffer (explicit scatter-gather), not in a multi-valued agent `HandlerResponse`. To make it real the buffer should accept a **batch** (`list<task>`) and scatter internally — a payload/contract change.

### 1.5 Integration audit (2026-06-14) — three facts that de-risk the build
- **WIT is already the single source of truth.** `agentos/crates/wit` parses one WIT def and *generates* the XML `PayloadSchema` (the validation template), the LLM JSON Schema, and the constrained-decode grammar. "Envelope as a WIT type" **extends an existing pattern** — not greenfield.
- **The tool-call surface is already decoupled from the wire.** `agentos/crates/agent/src/translate.rs` turns JSON tool calls → XML payload. The LLM never writes the wire, so the wire is free to change and the tool-call surface is unaffected by this work.
- **No WIT↔PayloadSchema drift seam.** `PayloadSchema` is generated from WIT, not hand-maintained — one less synchronization hazard.

---

## 2. The convergent shape

1.1–1.4 are the **same shape**: *one logical turn, many sub-operations, results gathered, provenance accumulated.* That is one contract, not four:

- **One canonical envelope** owned by rust-pipeline. Hierarchical address is the general case; flat `from/to/thread` is the degenerate single-instance case. Kills the second envelope and the double-enveloping bridge (1.1).
- **Provenance as `flags`** on that envelope, unioned at the (now single) build seam (1.2). Carrier here; policy in agentos; inert-until-enforced documented.
- **Handler contract** that expresses scatter-gather: the buffer takes a `list` and scatters across its semaphore; the agent stays dumb and serial (1.3, 1.4). `list<task>` is a native WIT type; clumsy in flat XML tags.
- **Materialization stays in agentos**, behind the `Runtime` trait that already exists (`router.rs:74` — `resolve_organism`/`allocate_instance`/`deliver`/`evict_instance`). That trait is the correct seam; it's just living one repo too high. Promote the *transport* (envelope, addressing, the hop, the trust boundary) into rust-pipeline; leave organism/kernel/eviction behind the trait.

---

## 3. Sequencing decision

**Eliminate XML — staged and encapsulated. Do not run two migrations.**

- The canonical envelope becomes a WIT type; serialization is owned and generated by rust-pipeline. The audit confirms this is an existing pattern (§1.5), so it's additive, not a rewrite.
- The correct envelope *types* (provenance `flags`, address `variant`, `list<task>` batch) are WIT-native. Designing them in XML structural-validation land first = designing them twice.
- **Therefore:** define the one canonical envelope as a WIT type; agentos adjusts **once** (drops platform `Envelope`, adopts the canonical WIT envelope value, keeps materialization behind `Runtime`).

**The order does not reverse on toolchain state.** The earlier "ship the carrier in XML now" escape had two legs — *(toolchain-not-ready)* AND *(provenance-security-urgent)* — and integration-claude confirmed the second leg is **false**: containment is launch-conditioned. Soft launch is by-invitation, Bob-as-concierge, no member-data dragnet; the adversarial surfaces that make the egress wall urgent (who-says probing, cross-context leak, corpus-writer wall) are V1/GA, gated behind the BHS partnership + dragnet. No live forcing function demands egress enforcement today. So provenance is **not** urgent → the XML-carrier escape never triggers.

### 3.1 Rider 1 — encapsulate serialization (the move that makes elimination safe)

The reason XML can be eliminated *without a second agentos migration* is encapsulation. The wire format must become **internal to rust-pipeline**:

- agentos must work with the **typed WIT envelope value**, not raw bytes. Today it touches `ValidatedPayload.xml` and the platform `body` as `Vec<u8>` (and `HandlerResponse::Send { payload_xml: Vec<u8> }`) — **that has to go.** Those are the leak points where the wire format escapes into agentos.
- **Encode-at-send, decode-at-receive, inside rust-pipeline.** Handlers/tools receive and return typed values; rust-pipeline owns the bytes on both sides of every hop.
- Consequence: the wire format is transparent to agentos, so swapping XML→binary later (§3.2 Commit 2) touches **zero agentos code**. This is the precondition for "migrate once." It is also a real handler-contract change (the `xml: Vec<u8>` / `payload_xml: Vec<u8>` surface), so it lands in Commit 1, not later.

### 3.2 Rider 2+3 — stage in two commits; the binary format is Commit 2's first task

"One contract" does **not** mean "one commit." Stage the elimination:

- **Commit 1 — typed envelope + provenance (GREEN-LIT, build now):**
  - Unified envelope as a WIT type. Collapse the two envelopes into one canonical type; hierarchical `Address` (`ringhub.bob[alice].calendar`) general, flat `from/to/thread` degenerate. Kills the double-enveloping at `runtime_impl.rs:151`. One canonical envelope serves both intra- and inter-instance, whether agentos keeps one instance or many.
  - Provenance as a **semantically-neutral `flags` carrier**, part of that same WIT type. Union at the `build_envelope` seam, accumulated per `thread_id`. Carry it; don't interpret it — mechanism, not policy.
  - Encapsulated serialization (§3.1) on the **WIT→XML serialization `crates/wit` already generates for free.** XML is still the bytes here — but it's now internal, generated, and on its way out. Provenance (the valuable/gating part) ships here, *not* waiting on the binary sub-decision.
- **Commit 2 — binary codec, deletes the XML step (fast-follow):**
  - **First task: scope the binary format.** "WIT on the wire" ≠ literal wasmtime Canonical ABI — CABI is for component-call lower/lift, awkward as a standalone wire codec. The real choice is a binary format over **WIT-derived serde types**: postcard / bincode / msgpack / cbor. Pick one (postcard and bincode are the compact Rust-native candidates; msgpack/cbor if cross-language wire compatibility matters).
  - Swap the encode/decode internals (§3.1) from XML to the chosen codec. Because serialization is encapsulated, this is an internal change — agentos is untouched, provenance (riding the typed value, not the bytes) survives intact.
  - **Hard constraint found while building Commit 1 (2026-06-14):** `payload-value` is **recursive** (a self-describing tree). The WebAssembly Component Model type system **forbids recursive types**, so `PayloadValue` is not a component-model type and cannot be wit-bindgen-generated. Two consequences: (a) the "generate from wire.wit" hardening (§D.3) applies only to the *non-recursive* header types (segment/address/provenance/meta/payload/envelope); `PayloadValue` stays hand-authored Rust, and drift is guarded by the `wire_wit_matches_rust_types` tripwire test instead. (b) It **reinforces** the codec choice — the binary format must be a self-describing serde format (cbor/msgpack) or schema-at-decode (the registry holds schemas by `tag`); the component ABI is doubly ruled out (recursive types + standalone-codec awkwardness).

### 3.3 Related contract extension — buffer `list<task>` batch-scatter

Separate axis from the serialization track above, but rides the same WIT contract: make the buffer accept a `list<task>` batch and scatter internally across its semaphore (1.4), restoring real parallelism. Design the contract with it in mind from the start; build it when the buffer parallelism is needed. Not on the Commit 1 / Commit 2 critical path.

### 3.4 Explicitly NOT in this work

- **Provenance policy** — what the bits *mean*, the egress/corpus-write rules. agentos's later layer (stamp/check/declassify). Commit 1 ships the neutral carrier only (1.2 trap: do not report "provenance done").

---

## 4. Status of the open questions

**Resolved (by integration-claude, 2026-06-14):**
- **XML stays or goes?** — goes. XML *elimination* is the goal (Daniel's call); the prior "keep XML serialization" framing was over-cautious and is withdrawn. XML survives only as Commit 1's transitional, encapsulated, generated serialization (§3.2).
- **Order vs. toolchain** — settled independent of toolchain (§3).
- **Provenance urgency** — not urgent; containment is launch-conditioned (§3). But provenance ships in Commit 1 anyway (it's the valuable part; it must not wait on the binary sub-decision).
- **WIT availability / timeline** — green light. Commit 1 rides the WIT→XML serialization `crates/wit` already generates (no new toolchain dependency). Start now.
- **Restore real parallelism / buffer batch-scatter** — same WIT contract, separate axis (§3.3). Not on the Commit 1/2 critical path.
- **Trust model under WIT** — settled (§5, §5.1). Per-hop check is structural trust-level-1; "dispatcher owns cortex" is a tested invariant.

**Still open:**
1. **Binary format for Commit 2** — postcard / bincode / msgpack / cbor over WIT-derived serde types (NOT literal CABI). Scoped as Commit 2's first task; does not block Commit 1.
2. Build-time: wire the model-output tool-call ingress as a first-class trust boundary (encode §5.1 as a test).
3. The `.wit` contract sketch for Commit 1 (envelope record + `flags` + reserved `list<task>` extension point) + the encapsulated handler-contract change (kill `ValidatedPayload.xml` / `payload_xml: Vec<u8>`) — the next deliverable.

---

## 5. Trust model under WIT — syntax is not trust

**Principle: grammar-constrained decoding (cortex) and WIT typed-decode both guarantee SYNTAX, never TRUST. Conflating them is the trap.**

- cortex's shim — detect impending tool call → switch to grammar-constrained mode → emit syntactically valid WIT — is a **correctness/liveness** feature (fewer malformed calls, fewer retries). It is **not** a security property. The output still lives in the model's text stream, and **text the model emits is untrusted ingress, grammar-valid or not.** A jailbroken/indirectly-injected model emits a *well-formed* malicious tool call as easily as a benign one — the grammar makes it parse, not legitimate.
- **Therefore the zero-trust re-injection boundary survives WIT unchanged.** "Decodes to the typed WIT record" is the new "well-formed XML" — trust-level-1 (structural), **not** a license to skip validate → route → peer-enforce → provenance → egress. Keep cortex's grammar-validity and the pipeline's trust check **separate** (mirrors the containment rule "never fuse classification and enforcement").

**Two untrusted ingress surfaces, both already self-feeding:**
1. **model → pipeline.** The emitted tool call is untrusted input; it crosses the same parse → validate → route → enforce → dispatch boundary as any ingress — not a privileged path. This is the containment §16.5 adversary (confused/injected in-band agent) named explicitly: untrusted content reaches the model → model emits a syntactically-perfect tool call. Designed answer = provenance taint + egress wall, not output inspection, not grammar.
2. **malicious wasm tool → pipeline.** A tool emitting bogus tool calls is structurally bounded by — none of it from grammar/typing, all of it structural:
   - sandbox capability limits (wasm),
   - output re-enters as **untrusted bytes**, re-validated on re-injection,
   - `Meta.from` set by the pipeline, **never self-reported** (`handler.rs` — a tool cannot forge origin),
   - peer/routing enforcement — reaches only declared peers,
   - provenance source-stamp it can't strip + egress destination allowlist.

**Consequence for the WIT contract:** the per-hop check is "decodes to the typed record" = structural trust-level-1 only; validation/routing/peer/provenance/egress remain downstream and mandatory. The model-output ingestion path must be a first-class ingress trust boundary, on equal footing with external bytes.

### 5.1 Invariant — the dispatcher owns cortex

**Cortex has no routing edge.** It is an inference resource the dispatcher *calls*: context in, text out. It cannot dispatch, cannot route, cannot invoke a tool — its only output channel is untrusted bytes into pipeline ingress. **Cortex proposes a tool call; the dispatcher disposes** (routing table, peers, provenance, egress).

- **Structural, not instructed.** There is no API from cortex to the router to be "trusted" or "misused" — the edge does not exist. Build-absence, per §4.3 Structural Impossibility / the actuator-wall "agent holds no credentials — structurally absent" rule.
- **Corollary — cortex may itself be treated as untrusted.** A backdoored model or a compromised cortex *binary* is contained by the same boundary as a jailbroken prompt or a malicious wasm tool: all three can only emit trust-level-1 text the dispatcher must still admit. Defense-in-depth for free, *because* the dispatcher owns cortex rather than trusting it.
- **Review rule.** Any code path that lets cortex route, dispatch, or invoke directly — bypassing pipeline ingress — is an architectural violation and must be rejected. Encode the absence as a tested invariant in the cortex↔pipeline integration contract; do not rely on a comment. The performance temptation ("it's already valid WIT, skip re-validation") is the exact path this invariant forbids.

---

## 6. Handoff to agentos (next session, not this one)

agentos adopts the canonical envelope **behind the `Runtime` trait that already exists** (`router.rs:74`), dropping its hand-rolled platform `Envelope`. Transport + envelope + addressing move into rust-pipeline; organism/kernel/materialization/eviction stay in agentos behind the trait. That seam is already the right shape (§2) — it just gets fed the canonical type. This is the next session's work; Commit 1 in rust-pipeline does not wait on it.

---

## 7. Federation — the cross-node tier (ACTIVE — RingHub is a live peer, 2026-06-15)

**Status corrected:** federation is **not** future. **RingHub ↔ AgentOS is a live cross-node edge** (RingHub sends to the node, the node replies to RingHub). The §7.1 design (per-node federation servers) is now a near-term phase, slotted between the rust-pipeline switchboard work and the agentos cutover.

### 7.1 Per-node federation servers (confirmed design, 2026-06-15)

**Each node runs a federation server; nodes mutually register; the address namespace IS the node.** AgentOS addresses `ringhub.bob` → its federation server sees namespace `ringhub` is a registered peer → transmits to RingHub's federation server → which forwards to RingHub's (Django) handling code. Symmetric: RingHub addresses `agentos.concierge[alice]` → AgentOS federation server → `switchboard.route` (local).

**The address hierarchy is the cross-node routing hierarchy** — each segment handled by its tier:
`namespace → node` (Federation) · `organism[key] → instance` (Switchboard) · `listener → handler` (Pipeline).

**Why a dedicated server (vs. a Transport hook on the switchboard):**
- **Switchboard stays pure-local.** It never sees a remote namespace — the federation server only hands it local-resolvable addresses. Phase 2 is untouched.
- **Translation boundary resolves the codec tension.** Internal wire (pipeline/switchboard) can be binary/Rust-native (postcard/bincode); the **federation wire** (between federation servers) is the cross-language one (cbor/msgpack/JSON) because that's the only Python↔Rust hop. Federation server decodes peer wire → canonical `Envelope` → switchboard. So Commit 2's internal codec is **not** constrained by RingHub being Python.
- **Trust localizes here.** Inbound from a peer = untrusted ingress: verify auth + stamp the edge provenance, *then* hand to the switchboard. A compromised peer is contained by the federation check + switchboard + pipeline re-validation (same "untrusted ingress behind the wall" discipline as cortex/tools, §5.1).

**Mechanism (the load-bearing detail):**
- **Egress** (`ringhub.bob` from a local handler): the **route stage checks `to.namespace()` against the peer directory first**. Remote namespace → dispatch to the **federation egress handler** (just-another-handler) → local federation server → peer. Local → route by `organism()` as today. The local-vs-remote decision lives in *routing* (one namespace lookup), not the switchboard.
- **Ingress** (peer → us): federation server receives + decodes + stamps + calls `switchboard.route(envelope)` with the local address.
- **Peer directory:** `namespace ↔ peer endpoint`, populated by mutual registration.

**Split:** rust-pipeline ships a generic `FederationServer` (peer link + directory + canonical-envelope framing + cross-language codec + trust stamp + inbound hook); RingHub implements the Python side of the **shared wire protocol**. Like any federated protocol — protocol shared, implementations per node.

**Trust model (Daniel, 2026-06-15): symmetric AEAD, hand-delivered pre-shared keys, NO auth-negotiation scheme yet.** A peer is authenticated *by possession of the shared key* — the AEAD verify gives authentication + integrity for free; a wrong-key or tampered frame fails to open (fail-closed). Concrete: **XChaCha20-Poly1305**, 256-bit per-peer key. Plaintext is the canonical envelope via the existing codec — **XML for now (already cross-language; RingHub-Python parses it)**, swappable to a binary cross-language codec with Commit 2. RingHub implements the identical scheme (libsodium/PyNaCl). Keys are per-peer `[u8;32]`, hand-configured in the peer directory — no rotation/PKI yet. **Opened messages are still untrusted ingress downstream** (decode → re-validate → switchboard); node-auth ≠ data-trust, so data provenance is still stamped (agentos policy).

**Node + message auth wired in now (integration, 2026-06-15 — cheap seam, hard to retrofit):** the frame carries an **AEAD-authenticated header** so stronger auth upgrades without a breaking reframe.
```
header = version:u8 ‖ auth_method:u8 ‖ sender_len:u16(BE) ‖ sender:utf8
frame  = header ‖ nonce:24 ‖ XChaCha20Poly1305(key, nonce, encode_envelope(env), aad=header)
```
version=1, auth_method=0 (pre-shared-key). **Node authentication:** the sender's node identity rides in the header, AEAD-bound (aad); `open(frame, &directory)` looks up *that sender's* key, verifies, and returns the **authenticated** sender — the claim can't be forged/relabeled. `auth_method` reserves the upgrade to per-node signatures (ed25519). **Message authentication:** the Poly1305 tag (over plaintext + header). `version`/`auth_method` are the retrofit escape hatches. **This is the exact wire spec RingHub's Python side implements.**

---

### 7.2 Original framing (kept for context)

It is **not** a separate architecture — it is the cross-*node* tier of the same address→switchboard→delivery path (sketch §C). Three tiers, one envelope, one address space:

| Tier | Delivery edge | Trust |
|---|---|---|
| intra-pipeline (thread) | local thread route | in-boundary |
| inter-pipeline, same process | switchboard, **by-value** hand-off | in-boundary |
| **inter-node / federated** | switchboard, **serialize → transport → node-auth** | **untrusted ingress, hardest tier** |

**Scaffolding already present:** the address **namespace** segment is the node identifier (`Address::namespace()` → `ringhub` for `ringhub.bob[alice]`; router has `NamespaceViolation`), and listeners already declare network `ports` (`{ port, protocol: https/http/ssh }`). What was never wired: namespace→*remote*-node resolution, cross-node envelope transport, and the trust boundary at that edge.

**Why it never got done = exactly what unification removes:** (1) two glued envelopes → no single wire type to ship; (2) no provenance → can't treat remote input as untrusted-but-labeled; (3) XML → no clean network codec. Commit 1 + Commit 2 dissolve all three. Federation then becomes "extend the switchboard delivery edge across the network + node-auth + stamp federation provenance" — not an epic.

**The one rider federation adds — trust hardens at the node edge:**
- A federated envelope is untrusted ingress, and the remote's claims are **not** trusted. The receiving node must not trust inbound `from` or `provenance`; it **stamps its own `external-node:X` provenance bit at federation ingress** (same pattern as `external-web`; the A2 "don't trust inbound provenance from outside the boundary" rule). A dropped/forged remote label can only over-restrict, never grant.
- This is **"the dispatcher owns cortex" generalized**: a federated peer is another untrusted source (like the model, like a malicious wasm tool), contained by the same validate → route → peer → provenance → egress wall. Federation adds reach, not a trust hole.
- Node identity/auth (mTLS, signed transport) attaches at the `ports` declaration — transport-layer, layered on, not the pipeline's job; but the boundary is named.

**Scoping — design-with-it-in-mind, build later (avoids a third migration):**
- the address namespace segment must be able to denote a remote node (already does logically);
- reserve a provenance convention for `external-node` stamps (agentos policy — just leave the bit-space);
- encapsulated serialization is transport-ready **by construction** (the point of §3.1).

Do not build federation in Commit 1/2; do not let the Commit 1 envelope foreclose it.

---

### 7.3 Delivery semantics — reliability is above the wire; "received" ≠ "accepted"

**The federation wire is fire-and-forget (best-effort), by design — like IP/Ethernet, not TCP.** `seal`/`send` move a frame; the wire makes no promise it arrives, is valid, or gets a reply. That is the correct base primitive — don't add reliability to it.

**Reliability is HOST policy, one step above the wire** (agentos on our side, RingHub on theirs): retry, dedup/idempotency, request/reply correlation, timeouts. rust-pipeline provides the *mechanism* (the `Transport::send -> Result` delivery-feedback hook + the envelope it carries); the host provides the *policy*.
- **Why the split lands exactly there:** **auth belongs in the wire** — it's what makes the channel a trust boundary, can't be bolted on above. **Reliability belongs above the wire** — orchestration, not a channel property (IP doesn't retry; TCP does). So per-message reliability metadata (a msg-id for dedup/correlation) lives in the **payload / host layer**, NOT on rust-pipeline's envelope meta — keep the wire pure transport (`thread` for conversation routing, nothing per-message). *(This is why the earlier "wire a msg_id into the envelope" idea was withdrawn — it belongs above the wire.)*
- The reliability *protocol* between RingHub ↔ AgentOS (acks/retries/dedup) is a symmetric **app-level agreement** designed by the host teams, layered on the federation wire — not rust-pipeline's to define.

**Three distinct acknowledgment points — "received" ≠ "accepted":**

| Moment | Means | Use |
|---|---|---|
| **Delivered** (received) | bytes accepted at the boundary, *before* the gauntlet | sender stops retrying |
| **Admitted** (accepted) | passed parse → WIT-validate → route → peer-check (earned trust-level-1) | "well-formed & now my responsibility" |
| **Processed** | a handler ran (the existing synthesized `ToolResponse`/ack on `None` + parent) | application outcome |

- **"Received" fires at *Delivered*, not at gauntlet-completion.** The pipeline *silently drops* failures (dead-letter posture), so coupling the delivery-ack to validity makes a *delivered-but-rejected* message indistinguishable from a *lost* one → retries break. **Delivery acknowledgment must be independent of validity.**
- **Trust-graded ack rule** (dissolves the "don't ack garbage" objection):
  - **Authenticated peer (federation):** ack *Delivered* — safe, because the AEAD already authenticated them (producing an openable frame *is* the proof of identity, so confirming receipt leaks nothing). May also surface *Admitted-vs-rejected* as debugging — a trusted peer sending malformed data is a bug, not an attacker to starve of information.
  - **Untrusted in-band content:** stay silent through the whole gauntlet — confirm nothing, explain no rejections.

Each layer asks the question it owns: transport → "did the bytes land," pipeline → "is it admissible," application → "did it get done."

**Idempotency (since at-least-once retry guarantees duplicates):** agentos already has a battle-tested idempotency cache (`crates/server/src/idempotency.rs`) — but it covers the **HTTP `POST /v1/messages` API only** (keyed on `service_token + client idempotency_key`, body-hash conflict, 24h TTL, replay/in-flight). The **federation edge does NOT pass through it.** The federation path needs its **own** dedup, modeled on that cache, keyed on **(peer namespace, federation msg-id)** — where the msg-id is the per-message id in the **payload/host layer** (consistent with "no msg-id on the envelope"; `thread` is per-*conversation*). RingHub stamps it; agentos dedups before `switchboard.route`. Host policy, both ends.

### 7.4 Origin namespace is an unforgeable edge stamp (re-homes `check_namespace`)

**A message does not get to assert its own namespace — the edge stamps it.** When a frame
arrives from authenticated peer `ringhub`, its origin namespace is `ringhub` *because of
which authenticated edge it arrived on*, not because the payload said so. Self-asserted
origin is the last-hop bug wearing a namespace costume (cf. §16.0 provenance, §17 consent):
origin is rooted, upstream, unforgeable.

This is the sibling of the provenance edge-stamp — same seam, same per-peer config, same
reason (the sender must not forge its origin):
- `inbound_provenance` → origin **data** label.
- origin **namespace** (the authenticated peer identity) → origin **authority/identity** label.

**Two parts — mechanism in rust-pipeline, policy in the host:**
1. **Stamp (mechanism, rust-pipeline):** `FederationServer::receive` calls `reroot_origin` to
   overwrite `from`'s leading namespace segment with the authenticated peer's. A peer can
   name *which of its agents* sent the message, never *which namespace*. ⇒ **`root` is
   structurally unstampable by any remote edge** — a remote `from` is *always* the peer
   namespace, so `root.*` can only be minted by local, in-process trusted entry points
   (operator, local trigger, local switchboard). That asymmetry **is** the tenant/admin
   isolation wall (§4.3 — a wall, not a sieve: there is no code path by which a ringhub-edged
   frame becomes `root`).
2. **Authorize (hook, host policy):** rust-pipeline exposes an `Authorizer { authorize(from, to) }`
   trait, called at the seam **after** stamping, **before** delivery, **fail-closed**. The
   *rule* (the `from × to` matrix — e.g. "a namespaced remote origin may never reach `root`")
   is the host's; the *seam* is rust-pipeline's. This **re-homes the deleted platform
   `check_namespace`** — into the federation authz seam, where the agentos session's
   intuition put it.

**Receive order:** `open` (authenticate) → **re-root `from`** → stamp provenance →
**`authorize(from, to)`** (on the still-namespaced `to`) → strip self-namespace → deliver.

**Authz keys on `Address::node()`** (the leading segment = federation node), **not**
`namespace()` (the organism-key heuristic for *local* instance addressing, unreliable for
`node.agent` without a key). On a re-rooted inbound `from`, `node()` is the authenticated peer.

---

## 8. Routing topology — route vs. dispatch, and the hierarchical resolver

Open question raised 2026-06-14: *what is the dispatcher/pipeline/router relationship when one node has several pipelines?* Answer: disentangle the two concerns the word "dispatcher" conflates.

- **Routing** = *deciding* where an envelope goes.
- **Dispatch** = *executing* the handler/agent for a local target.

**The address hierarchy IS the routing hierarchy.** `namespace.organism[key].listener` is peeled from the top; each segment is resolved by the scope that owns that level:

| Scope | Resolves | Lives | Trust edge |
|---|---|---|---|
| **Federation** | `namespace` → node | cross-node | untrusted (hardest) |
| **Node switchboard** | `organism[key]` → pipeline | above pipelines, one node | in-node |
| **Pipeline router** | `listener.tag` → handler | inside the pipeline | in-pipeline |

Routing is **recursive: resolve-or-escalate.** A pipeline routes its own internal hops (fast path); a non-local `to` escalates to the node switchboard; a non-local `namespace` escalates to federation. Same pattern, widening scope, hardening trust — like local-switch / inter-subnet-router / inter-AS-BGP.

**Decisions:**
1. **Dispatch belongs to the pipeline** (handler registry + kernel state + backpressure). One dispatcher per pipeline. Do not hoist it.
2. **The node switchboard ROUTES but never DISPATCHES.** It moves an envelope to the right pipeline's ingress, where *that* pipeline's dispatcher runs it. Dispatch-free = no actor capability, no acquisition edge — a dumb router (same posture as the watcher and "dispatcher owns cortex"). Pipelines **register** with it (`address-prefix → ingress`).
3. **The pipeline keeps its own internal router** (peer table, fast path). The switchboard is OFF the hot path for intra-pipeline traffic — it only sees a hop when the target is non-local. Preserves pipeline self-containment; avoids a central chokepoint.
4. **Agent-granularity is host policy, NOT architecture.** Per-agent pipelines are an *isolation* decision (separate kernel/KV/backpressure/eviction), not a *concurrency* one — tokio already multitasks within one dispatcher. The switchboard routes `organism[key] → pipeline` agnostic to whether a pipeline hosts one agent or many; the granularity lives behind the `Runtime` materialization hook. Keep routing decoupled from isolation granularity.
5. **The switchboard stays low-trust and simple.** Each pipeline re-validates at its own ingress (zero-trust, self-feeding), so the switchboard doesn't validate. A buggy/compromised switchboard is contained: the receiving pipeline's **peer enforcement** rejects a non-peer `from`, and **carried provenance** can't be stripped. Switchboard = directory + delivery, nothing else.

**Recommendation:** rust-pipeline owns the **switchboard abstraction** (a router over *registered pipelines*, keyed by the canonical address), with materialization behind the `Runtime` trait. That makes "register pipelines → route inside and between" a first-class rust-pipeline capability (resolves sketch §D-3). Decide this *model* now (it shapes how the address resolves); the switchboard need not *ship* in Commit 1.

### 8.0 Vocabulary (locked 2026-06-14)

The word "pipeline" was overloaded (library *and* instance). Resolved by naming the new fabric layer rather than renaming the accurate one:

| Layer | Name | Role |
|---|---|---|
| federation | **Federation** | `namespace` → node; cross-node, untrusted edge (§7) |
| fabric | **Switchboard** | `organism[key]` → pipeline; registers pipelines, routes between them; dispatch-free |
| instance | **Pipeline** | parse → validate → route → dispatch; one dispatcher, kernel state, backpressure |

`Pipeline` is **kept** — the staged instance genuinely is a pipeline (and `Stream` would mislead: in Rust that's a passive async iterator, not a stateful staged processor). The missing name was the router-over-pipelines → **Switchboard** ("Linksys"), self-documenting and consistent with agentos's existing `crates/platform`(router) vs `crates/pipeline`(instance) split. Repo name `rust-pipeline` unchanged (cosmetic). Refactor: additive — introduce `Switchboard`, touch router glue only; agentos's platform router becomes the Switchboard impl behind `Runtime`.

### 8.0.1 Topology reality check (code audit, 2026-06-15) — instances, not pipelines

A read-only sweep of agentos corrected a load-bearing assumption in §8: **agentos runs ONE pipeline, not a pipeline-per-instance.** The bridge `crates/pipeline/src/runtime_impl.rs:151` (`deliver`) injects every message into a **single `ingress_tx`**; "instances" are differentiated by kernel `thread_id` + listener name *within that one pipeline*. So:

- The switchboard fronts **one Pipeline and routes to many addressed INSTANCES** (materialized kernel threads that share listeners) — *not* a router over many pipeline handles. Read "register pipelines" throughout §8/§8.1 as "**materialize/route instances**"; the registration model (§8.1) still holds, keyed by instance address.
- **What the "ad-hoc switchboard" actually is, and what to remove:** agentos's `crates/platform` is (a) `InstanceRegistry` — address→instance directory with VMM tiers (Active/Shelved/Folded), idle eviction, snapshot persistence, per-instance buffers; (b) `Router::send_to` — materialize-on-route; (c) a **second `Envelope { to, from, body, buffer }`** plus the **bridge** that wraps the payload in it and *collapses* the hierarchical address to a listener name (`runtime_impl.rs:144,151`). **The wart to delete is (c)** — the second envelope and the double-enveloping bridge (the field-drop seam from §1.1). **(a) is legitimate agentos domain** (VMM/kernel) and stays put.
- **Corrected promotion:** a thin rust-pipeline `Switchboard` = address→instance routing + a `Materializer` hook (the promoted `Runtime` trait: resolve_organism / allocate_instance / deliver / evict), fronting one `Pipeline`, speaking the canonical `wire::Envelope`. This **eliminates the second envelope and the bridge** (the canonical envelope's hierarchical `Address` already carries everything the platform `Envelope` did; no collapse). agentos's `InstanceRegistry` + kernel become the `Materializer` impl behind the hook. The §8 principle is intact — rust-pipeline owns routing/directory *mechanics*, agentos owns materialization behind the trait — just **"pipelines" → "instances."**
- `wire::Address` was brought to parity with agentos's grammar (`organism`/`namespace`/`instance_key`/`buffer`/`instance_address`/`cache_keys`), and pipeline routing now resolves the listener via `organism()` (not the last segment) so buffered addresses like `bob[alice].dm` route to listener `bob`. Done 2026-06-15.

### 8.1 Registration / instantiation model

The switchboard directory is `address → ingress endpoint` (the switch ports). What varies is *when* a port is born — and the **address type decides**, it's not a taste call:

- **Static/singleton** (`console`, a gateway) → construct-then-register (eager, explicit).
- **Parameterized/ephemeral** (`bob[key]`, buffer children) → register a **template**; the switchboard instantiates lazily on first delivery, caches the live endpoint, evicts on idle. You *cannot* construct-then-register these — they don't exist until addressed, and may be unbounded (one `bob[member]` per member).

**One mechanism, not two:** `register_template(organism, factory, lifetime)`. Eager/static is just `Lifetime::Forever` + optional `prewarm()` — so eager-vs-lazy collapses into a lifetime/pre-warm policy, not a separate API.

- **Granularity:** registration is at the **template** level (`bob`, one call); the live directory caches at the **instance** level (`bob[alice]`, `bob[carol]`). Eviction removes the *instance*; the template registration persists. Mirrors the address (organism = template, key = instance) and the `shard_pattern`/`Lifetime` already on `OrganismMeta`.
- **You register a *running* endpoint**, not a cold `Pipeline`: `Pipeline(config) → .spawn() → RunningPipeline { ingress: Sender, _tasks, ctrl }`. The switchboard **owns the handle so it can evict** (drop → tasks shut down). That handle is the lifecycle hook materialization needs.
- **The factory IS the existing `Runtime` trait** (`resolve_organism` + `allocate_instance`); lazy instantiation = `deliver`-creates-if-missing. rust-pipeline owns directory + lifecycle mechanics; agentos owns *what a pipeline is made of*, behind the trait. Not new machinery — the existing materialization-on-routing, reframed as switchboard port-registration.

---

*Findings are read-only observations of current code as of 2026-06-14. Goal: eliminate XML, staged + encapsulated. Commit 1 (typed envelope + provenance, encapsulated, on generated WIT→XML) is GREEN-LIT; Commit 2 (binary codec, deletes XML) is the fast-follow, its format the first task. Nothing here is implemented yet.*
