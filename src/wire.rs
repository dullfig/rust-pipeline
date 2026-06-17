//! Canonical wire types — the Rust projection of `wire.wit`.
//!
//! This is the ONE envelope, shared by intra-instance hops, inter-pipeline
//! (switchboard) delivery, and later federation. Serialization (XML now, a binary
//! codec later) is **internal** to rust-pipeline — handlers and downstream crates work
//! with these typed values, never raw bytes (the encapsulation rider).
//!
//! `wire.wit` is the source of truth; these types mirror it 1:1. A hardening task will
//! replace the hand-authored types with wit-bindgen generation so there is no drift.
//!
//! Provenance is **carried and unioned, never interpreted** here — the bitset is opaque
//! to rust-pipeline; what a bit *means* is agentos policy.

use serde::{Deserialize, Serialize};

use crate::error::PipelineError;

// ── Address ──────────────────────────────────────────────────────────

/// Errors from address parsing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    #[error("empty address")]
    Empty,
    #[error("empty segment in address {0:?}")]
    EmptySegment(String),
    #[error("unbalanced '[' / ']' in address {0:?}")]
    Unbalanced(String),
    #[error("empty segment name in address {0:?}")]
    EmptyName(String),
}

impl From<AddressError> for PipelineError {
    fn from(e: AddressError) -> Self {
        PipelineError::EnvelopeParse(e.to_string())
    }
}

/// One segment of a hierarchical address: a name + optional instance key.
///
/// Cache-composition (`bob[main+alice]`) lives inside `key` as a `+`-joined string;
/// [`Segment::cache_keys`] splits it. A bare name with `key: None` is the degenerate
/// flat case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub name: String,
    pub key: Option<String>,
}

impl Segment {
    /// Cache-composition keys: the `key` split on `+`. Empty if no key.
    /// `bob[main+alice]` -> `["main", "alice"]`; `bob[alice]` -> `["alice"]`.
    pub fn cache_keys(&self) -> Vec<&str> {
        match &self.key {
            Some(k) => k.split('+').filter(|s| !s.is_empty()).collect(),
            None => Vec::new(),
        }
    }
}

/// A hierarchical agent address. `ringhub.bob[alice].calendar` -> 3 segments.
/// Flat single-listener routing is a one-segment address with no key — the degenerate
/// case that replaces today's flat `from`/`to` strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Address {
    pub segments: Vec<Segment>,
}

impl Address {
    /// Construct a flat (single-segment, no-key) address — the degenerate case.
    pub fn flat(name: impl Into<String>) -> Self {
        Address {
            segments: vec![Segment {
                name: name.into(),
                key: None,
            }],
        }
    }

    /// Parse `namespace.organism[key].buffer` into segments.
    ///
    /// Dots separate segments but are ignored inside `[...]` (a key may contain dots).
    pub fn parse(s: &str) -> Result<Address, AddressError> {
        if s.is_empty() {
            return Err(AddressError::Empty);
        }
        let mut segments = Vec::new();
        let mut depth: i32 = 0;
        let mut current = String::new();
        for ch in s.chars() {
            match ch {
                '[' => {
                    depth += 1;
                    current.push(ch);
                }
                ']' => {
                    depth -= 1;
                    if depth < 0 {
                        return Err(AddressError::Unbalanced(s.to_string()));
                    }
                    current.push(ch);
                }
                '.' if depth == 0 => {
                    segments.push(parse_segment(&current, s)?);
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        if depth != 0 {
            return Err(AddressError::Unbalanced(s.to_string()));
        }
        segments.push(parse_segment(&current, s)?);
        Ok(Address { segments })
    }

    /// The final segment name — kept for convenience/tests. NB: the *listener* a
    /// pipeline routes to is [`Address::organism`], NOT this; for a flat address they
    /// coincide, but `bob[alice].dm`'s target is "dm" while its organism is "bob".
    pub fn target(&self) -> Option<&str> {
        self.segments.last().map(|s| s.name.as_str())
    }

    /// Index of the organism segment: the first segment with a key, else segment 0.
    /// Everything before is namespace; everything after is buffer. (Matches agentos.)
    fn organism_index(&self) -> usize {
        self.segments
            .iter()
            .position(|s| s.key.is_some())
            .unwrap_or(0)
    }

    /// The organism name — the listener a pipeline routes to.
    /// `ringhub.bob[alice].dm` → "bob"; `bob[alice]` → "bob"; flat `echo` → "echo".
    pub fn organism(&self) -> Option<&str> {
        self.segments
            .get(self.organism_index())
            .map(|s| s.name.as_str())
    }

    /// The namespace prefix (the segment before the organism), if any.
    /// `ringhub.bob[alice]` → Some("ringhub"); `bob[alice]` → None.
    pub fn namespace(&self) -> Option<&str> {
        let idx = self.organism_index();
        (idx > 0).then(|| self.segments[0].name.as_str())
    }

    /// The instance key from the organism segment, if present (`bob[alice]` → "alice").
    pub fn instance_key(&self) -> Option<&str> {
        self.segments
            .get(self.organism_index())
            .and_then(|s| s.key.as_deref())
    }

    /// Cache-composition keys from the organism segment
    /// (`bob[main+alice]` → ["main", "alice"]).
    pub fn cache_keys(&self) -> Vec<&str> {
        self.segments
            .get(self.organism_index())
            .map(|s| s.cache_keys())
            .unwrap_or_default()
    }

    /// The buffer segment (first segment after the organism), if present
    /// (`bob[alice].dm` → "dm"). Buffers sub-route within an instance.
    pub fn buffer(&self) -> Option<&Segment> {
        self.segments.get(self.organism_index() + 1)
    }

    /// The instance-level address — namespace + organism[key], dropping any buffer.
    /// `ringhub.bob[alice].dm` → `ringhub.bob[alice]`.
    pub fn instance_address(&self) -> Address {
        let i = self.organism_index();
        Address {
            segments: self.segments[..=i].to_vec(),
        }
    }
}

fn parse_segment(raw: &str, whole: &str) -> Result<Segment, AddressError> {
    if raw.is_empty() {
        return Err(AddressError::EmptySegment(whole.to_string()));
    }
    if let Some(open) = raw.find('[') {
        let name = &raw[..open];
        if name.is_empty() {
            return Err(AddressError::EmptyName(whole.to_string()));
        }
        if !raw.ends_with(']') {
            return Err(AddressError::Unbalanced(whole.to_string()));
        }
        let key = &raw[open + 1..raw.len() - 1];
        Ok(Segment {
            name: name.to_string(),
            key: Some(key.to_string()),
        })
    } else {
        Ok(Segment {
            name: raw.to_string(),
            key: None,
        })
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, seg) in self.segments.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            f.write_str(&seg.name)?;
            if let Some(k) = &seg.key {
                write!(f, "[{k}]")?;
            }
        }
        Ok(())
    }
}

/// A bare name becomes a flat (degenerate) address — convenient for the common
/// single-listener case and for tests.
impl From<&str> for Address {
    fn from(s: &str) -> Self {
        Address::flat(s)
    }
}
impl From<String> for Address {
    fn from(s: String) -> Self {
        Address::flat(s)
    }
}

// ── Provenance ───────────────────────────────────────────────────────

/// Opaque provenance label-set — a 128-bit bitset with SET semantics.
///
/// rust-pipeline **carries and unions** provenance; it never interprets a bit. Union is
/// bitwise OR ([`Provenance::union`]); the egress predicate (agentos policy, not here)
/// is a mask AND ([`Provenance::intersects`]). A scalar/enum would re-introduce the
/// last-hop bug, so this is deliberately a set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Provenance {
    pub bits_lo: u64,
    pub bits_hi: u64,
}

impl Provenance {
    /// The empty set — carries nothing. NB: empty means *not cleared*, not *clean*;
    /// egress policy (agentos) treats absent provenance as fail-closed.
    pub const EMPTY: Provenance = Provenance {
        bits_lo: 0,
        bits_hi: 0,
    };

    /// A set containing a single bit `0..128`.
    pub fn from_bit(bit: u8) -> Provenance {
        let mut p = Provenance::EMPTY;
        p.set_bit(bit);
        p
    }

    /// Set bit `0..128` (no-op if out of range — bits ≥128 are not representable yet).
    pub fn set_bit(&mut self, bit: u8) {
        if bit < 64 {
            self.bits_lo |= 1u64 << bit;
        } else if bit < 128 {
            self.bits_hi |= 1u64 << (bit - 64);
        }
    }

    /// This set with `bit` added (builder style).
    pub fn with_bit(mut self, bit: u8) -> Provenance {
        self.set_bit(bit);
        self
    }

    /// The union of two sets (bitwise OR) — over-taint rather than leak.
    pub fn union(self, other: Provenance) -> Provenance {
        Provenance {
            bits_lo: self.bits_lo | other.bits_lo,
            bits_hi: self.bits_hi | other.bits_hi,
        }
    }

    /// Union `other` into this set in place.
    pub fn union_with(&mut self, other: Provenance) {
        self.bits_lo |= other.bits_lo;
        self.bits_hi |= other.bits_hi;
    }

    /// Whether this set carries no labels.
    pub fn is_empty(self) -> bool {
        self.bits_lo == 0 && self.bits_hi == 0
    }

    /// Whether `bit` is present.
    pub fn contains_bit(self, bit: u8) -> bool {
        if bit < 64 {
            self.bits_lo & (1u64 << bit) != 0
        } else if bit < 128 {
            self.bits_hi & (1u64 << (bit - 64)) != 0
        } else {
            false
        }
    }

    /// Whether this set shares any bit with `mask` (the egress mask-test primitive).
    pub fn intersects(self, mask: Provenance) -> bool {
        (self.bits_lo & mask.bits_lo) != 0 || (self.bits_hi & mask.bits_hi) != 0
    }
}

// ── Payload ──────────────────────────────────────────────────────────

/// A self-describing, format-agnostic decoded payload value. Keeps the pipeline generic
/// over per-operation schemas while keeping codec details out of downstream crates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PayloadValue {
    /// An ordered record of named fields.
    Rec(Vec<Field>),
    /// An ordered sequence of values.
    Seq(Vec<PayloadValue>),
    Text(String),
    Uint(u64),
    Sint(i64),
    Real(f64),
    Boolean(bool),
    Blob(Vec<u8>),
    /// Absence of a value.
    Nil,
}

impl PayloadValue {
    /// Construct a `Text` value.
    pub fn text(s: impl Into<String>) -> PayloadValue {
        PayloadValue::Text(s.into())
    }

    /// Construct a `Rec` from name/value pairs.
    pub fn record(fields: impl IntoIterator<Item = (impl Into<String>, PayloadValue)>) -> PayloadValue {
        PayloadValue::Rec(
            fields
                .into_iter()
                .map(|(n, v)| Field::new(n, v))
                .collect(),
        )
    }

    /// Look up a field by name in a `Rec` (first match). `None` for non-records.
    pub fn get(&self, name: &str) -> Option<&PayloadValue> {
        match self {
            PayloadValue::Rec(fields) => {
                fields.iter().find(|f| f.name == name).map(|f| &f.value)
            }
            _ => None,
        }
    }

    /// Borrow the inner string if this is `Text`.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            PayloadValue::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// One named field of a [`PayloadValue::Rec`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub value: PayloadValue,
}

impl Field {
    pub fn new(name: impl Into<String>, value: PayloadValue) -> Field {
        Field {
            name: name.into(),
            value,
        }
    }
}

/// A typed, tagged payload. `tag` selects the operation/schema; `value` is the decoded,
/// schema-validated content — never raw wire bytes at the handler boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    pub tag: String,
    pub value: PayloadValue,
}

impl Payload {
    pub fn new(tag: impl Into<String>, value: PayloadValue) -> Payload {
        Payload {
            tag: tag.into(),
            value,
        }
    }

    /// A payload that is a record with a single text field — the common case
    /// (`<Greeting><text>hi</text></Greeting>` ⇒ `single("Greeting", "text", "hi")`).
    pub fn single(tag: impl Into<String>, field: impl Into<String>, text: impl Into<String>) -> Payload {
        Payload {
            tag: tag.into(),
            value: PayloadValue::Rec(vec![Field::new(field, PayloadValue::Text(text.into()))]),
        }
    }
}

// ── Meta + Envelope ──────────────────────────────────────────────────

/// Trust-relevant envelope header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Meta {
    /// Last hop. Routing/audit ONLY — no enforcement predicate may read this.
    pub from: Address,
    /// Destination; `None` = unrouted/broadcast.
    pub to: Option<Address>,
    /// Opaque conversation/thread id (UUID string today).
    pub thread: String,
    /// Durable origin set; unioned into every envelope built downstream.
    pub provenance: Provenance,
}

/// The one canonical envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub meta: Meta,
    pub payload: Payload,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_flat_is_single_segment() {
        let a = Address::flat("greeter");
        assert_eq!(a.segments.len(), 1);
        assert_eq!(a.segments[0].name, "greeter");
        assert_eq!(a.segments[0].key, None);
        assert_eq!(a.target(), Some("greeter"));
        assert_eq!(a.to_string(), "greeter");
    }

    #[test]
    fn address_parse_full_path() {
        let a = Address::parse("ringhub.bob[alice].calendar").unwrap();
        assert_eq!(a.segments.len(), 3);
        assert_eq!(a.segments[0].name, "ringhub");
        assert_eq!(a.segments[1].name, "bob");
        assert_eq!(a.segments[1].key.as_deref(), Some("alice"));
        assert_eq!(a.segments[2].name, "calendar");
        assert_eq!(a.organism(), Some("bob"));
        assert_eq!(a.namespace(), Some("ringhub"));
        assert_eq!(a.instance_key(), Some("alice"));
        assert_eq!(a.buffer().map(|s| s.name.as_str()), Some("calendar"));
        assert_eq!(a.target(), Some("calendar"));
        // instance_address drops the buffer
        assert_eq!(a.instance_address().to_string(), "ringhub.bob[alice]");
        // round-trips through Display
        assert_eq!(a.to_string(), "ringhub.bob[alice].calendar");
    }

    #[test]
    fn address_cache_composition() {
        let a = Address::parse("bob[main+alice]").unwrap();
        assert_eq!(a.segments[0].cache_keys(), vec!["main", "alice"]);
        assert_eq!(a.cache_keys(), vec!["main", "alice"]);
        assert_eq!(a.organism(), Some("bob"));
        assert_eq!(a.instance_key(), Some("main+alice"));
        assert_eq!(a.namespace(), None);
        assert_eq!(a.buffer(), None);
    }

    #[test]
    fn address_rejects_garbage() {
        assert_eq!(Address::parse(""), Err(AddressError::Empty));
        assert!(matches!(
            Address::parse("a..b"),
            Err(AddressError::EmptySegment(_))
        ));
        assert!(matches!(
            Address::parse("bob[alice"),
            Err(AddressError::Unbalanced(_))
        ));
    }

    #[test]
    fn provenance_union_and_mask() {
        let a = Provenance::from_bit(3);
        let b = Provenance::from_bit(70); // crosses into bits_hi
        assert!(a.contains_bit(3));
        assert!(!a.contains_bit(70));
        assert!(b.contains_bit(70));

        let u = a.union(b);
        assert!(u.contains_bit(3) && u.contains_bit(70));
        assert!(!u.is_empty());

        // mask test: u intersects {3} but a clean set does not
        assert!(u.intersects(Provenance::from_bit(3)));
        assert!(!Provenance::EMPTY.intersects(Provenance::from_bit(3)));
    }

    #[test]
    fn provenance_empty_is_default() {
        assert!(Provenance::default().is_empty());
        assert!(Provenance::EMPTY.is_empty());
    }

    #[test]
    fn payload_value_record_access() {
        let pv = PayloadValue::Rec(vec![
            Field {
                name: "text".into(),
                value: PayloadValue::Text("hello".into()),
            },
            Field {
                name: "count".into(),
                value: PayloadValue::Uint(7),
            },
        ]);
        assert_eq!(pv.get("text").and_then(|v| v.as_text()), Some("hello"));
        assert_eq!(pv.get("count"), Some(&PayloadValue::Uint(7)));
        assert_eq!(pv.get("missing"), None);
    }

    #[test]
    fn envelope_builds() {
        let env = Envelope {
            meta: Meta {
                from: Address::flat("alice"),
                to: Some(Address::flat("bob")),
                thread: "t1".into(),
                provenance: Provenance::from_bit(1),
            },
            payload: Payload {
                tag: "Greeting".into(),
                value: PayloadValue::Rec(vec![Field {
                    name: "text".into(),
                    value: PayloadValue::Text("hi".into()),
                }]),
            },
        };
        assert_eq!(env.meta.from.target(), Some("alice"));
        assert_eq!(env.meta.to.as_ref().and_then(|a| a.target()), Some("bob"));
        assert!(env.meta.provenance.contains_bit(1));
        assert_eq!(env.payload.tag, "Greeting");
    }

    /// Extract the member names declared in a `record`/`variant` block of wire.wit.
    /// One member per comma; takes the identifier before `:` (records) or `(` (variants).
    fn wit_members(wit: &str, header: &str) -> Vec<String> {
        let start = wit
            .find(header)
            .unwrap_or_else(|| panic!("block not found in wire.wit: {header}"));
        let after = &wit[start + header.len()..];
        let end = after.find('}').expect("unterminated block in wire.wit");
        // strip line comments, then split members on commas
        let body: String = after[..end]
            .lines()
            .map(|l| l.find("//").map(|i| &l[..i]).unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        body.split(',')
            .filter_map(|m| {
                let token = m.trim().split([':', '(', ' ', '\n', '\t']).next().unwrap_or("").trim();
                (!token.is_empty()).then(|| token.to_string())
            })
            .collect()
    }

    /// Drift guard: wire.wit (the canonical spec) and these Rust types must declare the
    /// same members. Generation from wire.wit is impossible (payload-value is recursive →
    /// not a component-model type), so this tripwire is how we keep the two in sync. If
    /// you change a wire type, update wire.wit AND this list.
    #[test]
    fn wire_wit_matches_rust_types() {
        let wit = include_str!("../wire.wit");
        let check = |header: &str, expected: &[&str]| {
            let mut got = wit_members(wit, header);
            got.sort();
            let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
            want.sort();
            assert_eq!(got, want, "drift in `{header}` between wire.wit and src/wire.rs");
        };
        check("record segment {", &["name", "key"]);
        check("record address {", &["segments"]);
        check("record provenance {", &["bits-lo", "bits-hi"]);
        check("record meta {", &["from", "to", "thread", "provenance"]);
        check("record payload {", &["tag", "value"]);
        check("record field {", &["name", "value"]);
        check("record envelope {", &["meta", "payload"]);
        check(
            "variant payload-value {",
            &["rec", "seq", "text", "uint", "sint", "real", "boolean", "blob", "nil"],
        );
    }
}
