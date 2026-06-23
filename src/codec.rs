//! Envelope serialization — the encapsulated, swappable codec.
//!
//! `encode_envelope` / `decode_envelope` are the ONLY place wire bytes exist. Everything
//! upstream (handlers, the switchboard, downstream crates) works with the typed
//! [`Envelope`] value — never bytes. That encapsulation is what lets Commit 2 replace
//! this XML codec with a binary one (postcard/msgpack/cbor over the serde-derived types)
//! **without touching a single caller**.
//!
//! The transitional format is XML, keeping the pipeline's self-feeding bytes
//! human-readable and consistent with the repo's xml-pipeline lineage. The payload value
//! uses a uniform, kind-tagged grammar so any [`PayloadValue`] round-trips:
//!
//! ```xml
//! <message xmlns="...">
//!   <meta>
//!     <from>ringhub.bob[alice]</from>
//!     <to>carol</to>                 <!-- omitted if None -->
//!     <thread>uuid</thread>
//!     <provenance>00..01</provenance> <!-- 32 hex chars; omitted if empty -->
//!   </meta>
//!   <payload tag="Greeting">
//!     <v k="rec"><f n="text"><v k="text">hi</v></f></v>
//!   </payload>
//! </message>
//! ```

use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;

use crate::envelope::ENVELOPE_NS;
use crate::error::{PipelineError, PipelineResult};
use crate::wire::{Address, Envelope, Field, Meta, Payload, PayloadValue, Provenance};

// ── Encode ───────────────────────────────────────────────────────────

/// Serialize a typed [`Envelope`] to wire bytes (XML, transitional).
pub fn encode_envelope(env: &Envelope) -> PipelineResult<Vec<u8>> {
    let mut w = Writer::new(Vec::new());

    let mut msg = BytesStart::new("message");
    msg.push_attribute(("xmlns", ENVELOPE_NS));
    start(&mut w, msg)?;

    // <meta>
    start(&mut w, BytesStart::new("meta"))?;
    text_el(&mut w, "from", &env.meta.from.to_string())?;
    if let Some(to) = &env.meta.to {
        text_el(&mut w, "to", &to.to_string())?;
    }
    text_el(&mut w, "thread", &env.meta.thread)?;
    if !env.meta.provenance.is_empty() {
        text_el(&mut w, "provenance", &prov_to_hex(env.meta.provenance))?;
    }
    end(&mut w, "meta")?;

    // <payload tag="...">
    let mut pl = BytesStart::new("payload");
    pl.push_attribute(("tag", env.payload.tag.as_str()));
    start(&mut w, pl)?;
    write_value(&mut w, &env.payload.value)?;
    end(&mut w, "payload")?;

    end(&mut w, "message")?;
    Ok(w.into_inner())
}

fn write_value(w: &mut Writer<Vec<u8>>, v: &PayloadValue) -> PipelineResult<()> {
    match v {
        PayloadValue::Rec(fields) => {
            start(w, v_start("rec"))?;
            for f in fields {
                let mut fe = BytesStart::new("f");
                fe.push_attribute(("n", f.name.as_str()));
                start(w, fe)?;
                write_value(w, &f.value)?;
                end(w, "f")?;
            }
            end(w, "v")?;
        }
        PayloadValue::Seq(items) => {
            start(w, v_start("seq"))?;
            for it in items {
                write_value(w, it)?;
            }
            end(w, "v")?;
        }
        PayloadValue::Text(s) => scalar(w, "text", s)?,
        PayloadValue::Uint(n) => scalar(w, "uint", &n.to_string())?,
        PayloadValue::Sint(n) => scalar(w, "sint", &n.to_string())?,
        PayloadValue::Real(x) => scalar(w, "real", &x.to_string())?,
        PayloadValue::Boolean(b) => scalar(w, "bool", if *b { "true" } else { "false" })?,
        PayloadValue::Blob(bytes) => scalar(w, "blob", &to_hex(bytes))?,
        PayloadValue::Nil => {
            // self-closing <v k="nil"/>
            w.write_event(Event::Empty(v_start("nil")))
                .map_err(ser)?;
        }
    }
    Ok(())
}

fn v_start(kind: &str) -> BytesStart<'static> {
    let mut e = BytesStart::new("v");
    e.push_attribute(("k", kind));
    e.into_owned()
}

fn scalar(w: &mut Writer<Vec<u8>>, kind: &str, text: &str) -> PipelineResult<()> {
    start(w, v_start(kind))?;
    w.write_event(Event::Text(BytesText::new(text))).map_err(ser)?;
    end(w, "v")
}

// ── Decode ───────────────────────────────────────────────────────────

/// Parse wire bytes (XML, transitional) into a typed [`Envelope`].
///
/// Absent `<provenance>` ⇒ empty set (fail-closed: empty means *not cleared*, not
/// *clean*; egress policy treats it as such). Absent `<to>` ⇒ `None`.
pub fn decode_envelope(raw: &[u8]) -> PipelineResult<Envelope> {
    let mut reader = Reader::from_reader(raw);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut from: Option<Address> = None;
    let mut to: Option<Address> = None;
    let mut thread: Option<String> = None;
    let mut provenance = Provenance::EMPTY;
    let mut payload: Option<Payload> = None;
    let mut found_message = false;

    loop {
        match reader.read_event_into(&mut buf).map_err(parse)? {
            Event::Start(e) => match e.name().as_ref() {
                b"message" => found_message = true,
                b"meta" => {
                    read_meta(&mut reader, &mut from, &mut to, &mut thread, &mut provenance)?;
                }
                b"payload" => {
                    let tag = get_attr(&e, b"tag").unwrap_or_default();
                    let value = expect_value(&mut reader)?;
                    expect_end(&mut reader, b"payload")?;
                    payload = Some(Payload { tag, value });
                }
                other => {
                    return Err(parse_msg(format!(
                        "unexpected element <{}>",
                        String::from_utf8_lossy(other)
                    )))
                }
            },
            Event::End(e) if e.name().as_ref() == b"message" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !found_message {
        return Err(parse_msg("no <message> element".into()));
    }
    Ok(Envelope {
        meta: Meta {
            from: from.ok_or(PipelineError::MissingMeta("from"))?,
            to,
            thread: thread.ok_or(PipelineError::MissingMeta("thread"))?,
            provenance,
        },
        payload: payload.ok_or(PipelineError::MissingMeta("payload"))?,
    })
}

fn read_meta(
    reader: &mut Reader<&[u8]>,
    from: &mut Option<Address>,
    to: &mut Option<Address>,
    thread: &mut Option<String>,
    provenance: &mut Provenance,
) -> PipelineResult<()> {
    let mut buf = Vec::new();
    let mut field: Option<Vec<u8>> = None;
    loop {
        match reader.read_event_into(&mut buf).map_err(parse)? {
            Event::Start(e) => field = Some(e.name().as_ref().to_vec()),
            Event::Text(t) => {
                if let Some(name) = &field {
                    let s = t.unescape().map_err(parse)?.trim().to_string();
                    match name.as_slice() {
                        b"from" => *from = Some(Address::parse(&s)?),
                        b"to" => *to = Some(Address::parse(&s)?),
                        b"thread" => *thread = Some(s),
                        b"provenance" => *provenance = prov_from_hex(&s)?,
                        _ => {}
                    }
                }
            }
            Event::End(e) => {
                if e.name().as_ref() == b"meta" {
                    return Ok(());
                }
                field = None;
            }
            Event::Eof => return Err(parse_msg("unexpected EOF in <meta>".into())),
            _ => {}
        }
        buf.clear();
    }
}

/// Read the next `<v>` element (skipping whitespace) and parse it into a value.
fn expect_value(reader: &mut Reader<&[u8]>) -> PipelineResult<PayloadValue> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(parse)? {
            Event::Start(e) if e.name().as_ref() == b"v" => {
                let kind = get_attr(&e, b"k").unwrap_or_default();
                return parse_value(reader, &kind);
            }
            Event::Empty(e) if e.name().as_ref() == b"v" => {
                let kind = get_attr(&e, b"k").unwrap_or_default();
                return Ok(empty_value(&kind));
            }
            Event::Text(_) => {}
            Event::Eof => return Err(parse_msg("expected <v>, found EOF".into())),
            other => {
                return Err(parse_msg(format!("expected <v>, found {other:?}")));
            }
        }
        buf.clear();
    }
}

/// Parse the body of a `<v k=kind>` whose start tag was already consumed.
fn parse_value(reader: &mut Reader<&[u8]>, kind: &str) -> PipelineResult<PayloadValue> {
    match kind {
        "rec" => {
            let mut fields = Vec::new();
            let mut buf = Vec::new();
            loop {
                match reader.read_event_into(&mut buf).map_err(parse)? {
                    Event::Start(e) if e.name().as_ref() == b"f" => {
                        let name = get_attr(&e, b"n").unwrap_or_default();
                        let value = expect_value(reader)?;
                        expect_end(reader, b"f")?;
                        fields.push(Field { name, value });
                    }
                    Event::End(e) if e.name().as_ref() == b"v" => break,
                    Event::Text(_) => {}
                    Event::Eof => return Err(parse_msg("EOF in <v k=rec>".into())),
                    _ => {}
                }
                buf.clear();
            }
            Ok(PayloadValue::Rec(fields))
        }
        "seq" => {
            let mut items = Vec::new();
            let mut buf = Vec::new();
            loop {
                match reader.read_event_into(&mut buf).map_err(parse)? {
                    Event::Start(e) if e.name().as_ref() == b"v" => {
                        let k = get_attr(&e, b"k").unwrap_or_default();
                        items.push(parse_value(reader, &k)?);
                    }
                    Event::Empty(e) if e.name().as_ref() == b"v" => {
                        let k = get_attr(&e, b"k").unwrap_or_default();
                        items.push(empty_value(&k));
                    }
                    Event::End(e) if e.name().as_ref() == b"v" => break,
                    Event::Text(_) => {}
                    Event::Eof => return Err(parse_msg("EOF in <v k=seq>".into())),
                    _ => {}
                }
                buf.clear();
            }
            Ok(PayloadValue::Seq(items))
        }
        "text" => Ok(PayloadValue::Text(read_scalar(reader)?)),
        "uint" => {
            let s = read_scalar(reader)?;
            Ok(PayloadValue::Uint(s.parse().map_err(|_| {
                parse_msg(format!("invalid uint {s:?}"))
            })?))
        }
        "sint" => {
            let s = read_scalar(reader)?;
            Ok(PayloadValue::Sint(s.parse().map_err(|_| {
                parse_msg(format!("invalid sint {s:?}"))
            })?))
        }
        "real" => {
            let s = read_scalar(reader)?;
            Ok(PayloadValue::Real(s.parse().map_err(|_| {
                parse_msg(format!("invalid real {s:?}"))
            })?))
        }
        "bool" => {
            let s = read_scalar(reader)?;
            Ok(PayloadValue::Boolean(s == "true"))
        }
        "blob" => Ok(PayloadValue::Blob(from_hex(&read_scalar(reader)?)?)),
        "nil" => {
            // shouldn't normally reach here (nil is self-closing), but tolerate it
            let _ = read_scalar(reader)?;
            Ok(PayloadValue::Nil)
        }
        other => Err(parse_msg(format!("unknown value kind {other:?}"))),
    }
}

/// A value for a self-closing `<v k=kind/>` (empty content).
fn empty_value(kind: &str) -> PayloadValue {
    match kind {
        "rec" => PayloadValue::Rec(Vec::new()),
        "seq" => PayloadValue::Seq(Vec::new()),
        "text" => PayloadValue::Text(String::new()),
        "blob" => PayloadValue::Blob(Vec::new()),
        _ => PayloadValue::Nil,
    }
}

/// Read text content up to the closing `</v>`.
fn read_scalar(reader: &mut Reader<&[u8]>) -> PipelineResult<String> {
    let mut buf = Vec::new();
    let mut s = String::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(parse)? {
            Event::Text(t) => s.push_str(&t.unescape().map_err(parse)?),
            Event::End(e) if e.name().as_ref() == b"v" => return Ok(s),
            Event::Eof => return Err(parse_msg("EOF in scalar <v>".into())),
            _ => {}
        }
        buf.clear();
    }
}

fn expect_end(reader: &mut Reader<&[u8]>, name: &[u8]) -> PipelineResult<()> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(parse)? {
            Event::End(e) if e.name().as_ref() == name => return Ok(()),
            Event::Text(_) => {}
            Event::Eof => {
                return Err(parse_msg(format!(
                    "expected </{}>, found EOF",
                    String::from_utf8_lossy(name)
                )))
            }
            other => {
                return Err(parse_msg(format!(
                    "expected </{}>, found {other:?}",
                    String::from_utf8_lossy(name)
                )))
            }
        }
        buf.clear();
    }
}

// ── Small helpers ────────────────────────────────────────────────────

fn start(w: &mut Writer<Vec<u8>>, e: BytesStart) -> PipelineResult<()> {
    w.write_event(Event::Start(e)).map_err(ser)
}
fn end(w: &mut Writer<Vec<u8>>, name: &str) -> PipelineResult<()> {
    w.write_event(Event::End(BytesEnd::new(name))).map_err(ser)
}
fn text_el(w: &mut Writer<Vec<u8>>, tag: &str, text: &str) -> PipelineResult<()> {
    start(w, BytesStart::new(tag))?;
    w.write_event(Event::Text(BytesText::new(text))).map_err(ser)?;
    end(w, tag)
}

fn get_attr(e: &BytesStart, key: &[u8]) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| a.key.as_ref() == key)
        .map(|a| String::from_utf8_lossy(&a.value).into_owned())
}

fn prov_to_hex(p: Provenance) -> String {
    format!("{:016x}{:016x}", p.bits_hi, p.bits_lo)
}
fn prov_from_hex(s: &str) -> PipelineResult<Provenance> {
    let s = s.trim();
    if s.len() != 32 {
        return Err(parse_msg(format!("provenance must be 32 hex chars, got {}", s.len())));
    }
    let hi = u64::from_str_radix(&s[..16], 16).map_err(|_| parse_msg("bad provenance hex".into()))?;
    let lo = u64::from_str_radix(&s[16..], 16).map_err(|_| parse_msg("bad provenance hex".into()))?;
    Ok(Provenance { bits_lo: lo, bits_hi: hi })
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
fn from_hex(s: &str) -> PipelineResult<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(parse_msg("blob hex must have even length".into()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| parse_msg("bad blob hex".into())))
        .collect()
}

fn ser<E: std::fmt::Display>(e: E) -> PipelineError {
    PipelineError::Serialization(e.to_string())
}
fn parse<E: std::fmt::Display>(e: E) -> PipelineError {
    PipelineError::EnvelopeParse(e.to_string())
}
fn parse_msg(m: String) -> PipelineError {
    PipelineError::EnvelopeParse(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Envelope {
        Envelope {
            meta: Meta {
                from: Address::parse("ringhub.bob[alice]").unwrap(),
                to: Some(Address::flat("carol")),
                thread: "550e8400-e29b-41d4-a716-446655440000".into(),
                provenance: Provenance::from_bit(3).union(Provenance::from_bit(70)),
            },
            payload: Payload {
                tag: "Greeting".into(),
                value: PayloadValue::Rec(vec![
                    Field {
                        name: "text".into(),
                        value: PayloadValue::Text("Hello, world!".into()),
                    },
                    Field {
                        name: "count".into(),
                        value: PayloadValue::Uint(42),
                    },
                    Field {
                        name: "nested".into(),
                        value: PayloadValue::Rec(vec![Field {
                            name: "flag".into(),
                            value: PayloadValue::Boolean(true),
                        }]),
                    },
                    Field {
                        name: "items".into(),
                        value: PayloadValue::Seq(vec![
                            PayloadValue::Sint(-5),
                            PayloadValue::Real(1.5),
                            PayloadValue::Nil,
                        ]),
                    },
                    Field {
                        name: "raw".into(),
                        value: PayloadValue::Blob(vec![0xde, 0xad, 0xbe, 0xef]),
                    },
                ]),
            },
        }
    }

    #[test]
    fn round_trip_complex() {
        let env = sample();
        let bytes = encode_envelope(&env).unwrap();
        let back = decode_envelope(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn round_trip_minimal() {
        // no `to`, empty provenance, nil payload
        let env = Envelope {
            meta: Meta {
                from: Address::flat("system"),
                to: None,
                thread: "t1".into(),
                provenance: Provenance::EMPTY,
            },
            payload: Payload {
                tag: "Ping".into(),
                value: PayloadValue::Nil,
            },
        };
        let bytes = encode_envelope(&env).unwrap();
        let back = decode_envelope(&bytes).unwrap();
        assert_eq!(env, back);
        // empty provenance is omitted from the wire
        assert!(!String::from_utf8_lossy(&bytes).contains("provenance"));
        // absent `to` stays None
        assert!(back.meta.to.is_none());
    }

    #[test]
    fn provenance_survives_round_trip() {
        let env = sample();
        let bytes = encode_envelope(&env).unwrap();
        let back = decode_envelope(&bytes).unwrap();
        assert!(back.meta.provenance.contains_bit(3));
        assert!(back.meta.provenance.contains_bit(70));
        assert!(!back.meta.provenance.contains_bit(5));
    }

    #[test]
    fn garbage_rejected() {
        assert!(decode_envelope(b"not xml").is_err());
        assert!(decode_envelope(b"<message><meta></meta></message>").is_err()); // missing from/thread/payload
    }
}
