//! Envelope parsing, construction, and serialization.
//!
//! The universal envelope format:
//! ```xml
//! <message xmlns="https://xml-pipeline.org/ns/envelope/v1">
//!   <meta>
//!     <from>sender</from>
//!     <to>receiver</to>
//!     <thread>uuid</thread>
//!   </meta>
//!   <Payload xmlns="">...</Payload>
//! </message>
//! ```
//!
//! Envelopes are parsed once at ingress and flow through the pipeline as typed
//! structs. They represent the first level of earned trust: the raw bytes were
//! well-formed XML with the required metadata structure.

use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;

use crate::error::{PipelineError, PipelineResult};

/// The envelope namespace URI (matches xml-pipeline).
pub const ENVELOPE_NS: &str = "https://xml-pipeline.org/ns/envelope/v1";

// ── Types ────────────────────────────────────────────────────────────

/// Unique agent identifier (listener name).
pub type AgentId = String;

/// Opaque thread identifier (UUID string).
pub type ThreadId = String;

/// Parsed envelope — first trust boundary.
///
/// If you have an `Envelope`, it means the raw bytes were valid XML
/// with a `<message>` root containing `<meta>` with required fields.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub meta: Meta,
    /// Raw XML bytes of the payload element (everything inside <message> after <meta>).
    /// This stays as raw bytes until XSD validation promotes it further.
    pub payload_raw: Vec<u8>,
    /// The tag name of the payload element (e.g., "Greeting", "GreetingResponse").
    pub payload_tag: String,
}

/// Envelope metadata — extracted from `<meta>` block.
#[derive(Debug, Clone, PartialEq)]
pub struct Meta {
    pub from: AgentId,
    pub to: Option<AgentId>,
    pub thread: ThreadId,
}

// ── Parsing ──────────────────────────────────────────────────────────

/// Parse raw bytes into an `Envelope`.
///
/// This is the ingress boundary. Raw untrusted bytes go in, and either
/// a typed `Envelope` comes out (trust earned) or an error (trust denied).
pub fn parse_envelope(raw: &[u8]) -> PipelineResult<Envelope> {
    let mut reader = Reader::from_reader(raw);
    reader.config_mut().trim_text(true);

    let mut in_meta = false;
    let mut current_field: Option<String> = None;
    let mut from_id: Option<String> = None;
    let mut to_id: Option<String> = None;
    let mut thread_id: Option<String> = None;
    let mut found_message = false;
    let mut meta_done = false;
    let mut payload_raw: Vec<u8> = Vec::new();
    let mut payload_tag = String::new();

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let local_name = local_name(e.name().as_ref());

                if !found_message {
                    if local_name == "message" {
                        found_message = true;
                    } else {
                        return Err(PipelineError::EnvelopeParse(format!(
                            "expected <message>, found <{local_name}>"
                        )));
                    }
                } else if !meta_done && local_name == "meta" {
                    in_meta = true;
                } else if in_meta {
                    current_field = Some(local_name.to_string());
                } else {
                    // We're past meta — this is the payload element.
                    // Capture everything from here to the matching end tag.
                    payload_tag = local_name.to_string();
                    payload_raw = capture_element_bytes(&mut reader, e)?;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_meta {
                    if let Some(ref field) = current_field {
                        let text = e
                            .unescape()
                            .map_err(|err| PipelineError::EnvelopeParse(err.to_string()))?
                            .trim()
                            .to_string();
                        match field.as_str() {
                            "from" => from_id = Some(text),
                            "to" => to_id = Some(text),
                            "thread" => thread_id = Some(text),
                            _ => {} // ignore unknown meta fields (extensibility)
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let local_name = local_name(e.name().as_ref());
                if in_meta && local_name == "meta" {
                    in_meta = false;
                    meta_done = true;
                    current_field = None;
                } else if in_meta {
                    current_field = None;
                } else if local_name == "message" {
                    break;
                }
            }
            Ok(Event::Empty(ref e)) => {
                if found_message && meta_done {
                    // Self-closing payload like <Ping/>
                    let local = local_name(e.name().as_ref());
                    payload_tag = local.to_string();
                    // Reconstruct as bytes
                    let mut w = Writer::new(Vec::new());
                    w.write_event(Event::Empty(e.clone()))
                        .map_err(|err| PipelineError::EnvelopeParse(err.to_string()))?;
                    payload_raw = w.into_inner();
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(PipelineError::EnvelopeParse(e.to_string())),
            _ => {}
        }
        buf.clear();
    }

    if !found_message {
        return Err(PipelineError::EnvelopeParse(
            "no <message> element found".into(),
        ));
    }

    let from = from_id.ok_or(PipelineError::MissingMeta("from"))?;
    let thread = thread_id.ok_or(PipelineError::MissingMeta("thread"))?;

    Ok(Envelope {
        meta: Meta {
            from,
            to: to_id,
            thread,
        },
        payload_raw,
        payload_tag,
    })
}

/// Build an envelope from parts and serialize to bytes.
///
/// This is the re-injection serialization path. Handler responses
/// get wrapped in an envelope and serialized back to raw bytes —
/// losing all trust, ready to re-enter the pipeline from the top.
pub fn build_envelope(
    from: &str,
    to: &str,
    thread: &str,
    payload_xml: &[u8],
) -> PipelineResult<Vec<u8>> {
    let mut writer = Writer::new(Vec::new());

    // <message xmlns="...">
    let mut msg_start = BytesStart::new("message");
    msg_start.push_attribute(("xmlns", ENVELOPE_NS));
    writer
        .write_event(Event::Start(msg_start))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;

    // <meta>
    writer
        .write_event(Event::Start(BytesStart::new("meta")))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;

    // <from>
    write_text_element(&mut writer, "from", from)?;
    // <to>
    write_text_element(&mut writer, "to", to)?;
    // <thread>
    write_text_element(&mut writer, "thread", thread)?;

    // </meta>
    writer
        .write_event(Event::End(BytesEnd::new("meta")))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;

    // Payload (raw XML bytes, injected verbatim)
    writer
        .get_mut()
        .extend_from_slice(payload_xml);

    // </message>
    writer
        .write_event(Event::End(BytesEnd::new("message")))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;

    Ok(writer.into_inner())
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Extract the local name from a possibly-namespaced tag.
fn local_name(name: &[u8]) -> String {
    let s = std::str::from_utf8(name).unwrap_or("");
    // Handle "ns:local" prefixed names
    if let Some(pos) = s.rfind(':') {
        s[pos + 1..].to_string()
    } else {
        s.to_string()
    }
}

/// Write a simple `<tag>text</tag>` element.
fn write_text_element(
    writer: &mut Writer<Vec<u8>>,
    tag: &str,
    text: &str,
) -> PipelineResult<()> {
    writer
        .write_event(Event::Start(BytesStart::new(tag)))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;
    writer
        .write_event(Event::Text(BytesText::new(text)))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;
    writer
        .write_event(Event::End(BytesEnd::new(tag)))
        .map_err(|e| PipelineError::Serialization(e.to_string()))?;
    Ok(())
}

/// Capture the full XML bytes of an element (including the start tag we already consumed).
/// Reads until the matching end tag, tracking nesting depth.
fn capture_element_bytes(
    reader: &mut Reader<&[u8]>,
    start: &BytesStart,
) -> PipelineResult<Vec<u8>> {
    let mut writer = Writer::new(Vec::new());

    // Write the opening tag we already consumed
    writer
        .write_event(Event::Start(start.clone()))
        .map_err(|e| PipelineError::EnvelopeParse(e.to_string()))?;

    let tag_name = start.name();
    let mut depth: u32 = 1;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                if e.name() == tag_name {
                    depth += 1;
                }
                writer
                    .write_event(Event::Start(e.clone()))
                    .map_err(|e| PipelineError::EnvelopeParse(e.to_string()))?;
            }
            Ok(Event::End(ref e)) => {
                if e.name() == tag_name {
                    depth -= 1;
                }
                writer
                    .write_event(Event::End(e.clone()))
                    .map_err(|e| PipelineError::EnvelopeParse(e.to_string()))?;
                if depth == 0 {
                    break;
                }
            }
            Ok(Event::Eof) => {
                // Unexpected EOF inside payload
                return Err(PipelineError::EnvelopeParse(
                    "unexpected EOF inside payload element".into(),
                ));
            }
            Ok(event) => {
                writer
                    .write_event(event)
                    .map_err(|e| PipelineError::EnvelopeParse(e.to_string()))?;
            }
            Err(e) => return Err(PipelineError::EnvelopeParse(e.to_string())),
        }
        buf.clear();
    }

    Ok(writer.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_envelope() -> Vec<u8> {
        br#"<message xmlns="https://xml-pipeline.org/ns/envelope/v1">
  <meta>
    <from>greeter</from>
    <to>shouter</to>
    <thread>550e8400-e29b-41d4-a716-446655440000</thread>
  </meta>
  <Greeting xmlns="">
    <text>Hello, world!</text>
  </Greeting>
</message>"#
            .to_vec()
    }

    #[test]
    fn parse_valid_envelope() {
        let env = parse_envelope(&sample_envelope()).unwrap();
        assert_eq!(env.meta.from, "greeter");
        assert_eq!(env.meta.to.as_deref(), Some("shouter"));
        assert_eq!(
            env.meta.thread,
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(env.payload_tag, "Greeting");
        let payload_str = String::from_utf8_lossy(&env.payload_raw);
        assert!(payload_str.contains("Hello, world!"));
    }

    #[test]
    fn parse_missing_from() {
        let xml = br#"<message xmlns="https://xml-pipeline.org/ns/envelope/v1">
  <meta>
    <to>shouter</to>
    <thread>abc</thread>
  </meta>
  <Payload/>
</message>"#;
        let err = parse_envelope(xml).unwrap_err();
        assert!(err.to_string().contains("from"));
    }

    #[test]
    fn parse_missing_thread() {
        let xml = br#"<message xmlns="https://xml-pipeline.org/ns/envelope/v1">
  <meta>
    <from>greeter</from>
  </meta>
  <Payload/>
</message>"#;
        let err = parse_envelope(xml).unwrap_err();
        assert!(err.to_string().contains("thread"));
    }

    #[test]
    fn parse_no_to_is_ok() {
        let xml = br#"<message xmlns="https://xml-pipeline.org/ns/envelope/v1">
  <meta>
    <from>console</from>
    <thread>abc-123</thread>
  </meta>
  <Broadcast xmlns=""><msg>hi</msg></Broadcast>
</message>"#;
        let env = parse_envelope(xml).unwrap();
        assert!(env.meta.to.is_none());
        assert_eq!(env.payload_tag, "Broadcast");
    }

    #[test]
    fn build_and_reparse_roundtrip() {
        let payload = b"<Greeting><text>hi</text></Greeting>";
        let bytes = build_envelope(
            "alice",
            "bob",
            "thread-1",
            payload,
        )
        .unwrap();

        let env = parse_envelope(&bytes).unwrap();
        assert_eq!(env.meta.from, "alice");
        assert_eq!(env.meta.to.as_deref(), Some("bob"));
        assert_eq!(env.meta.thread, "thread-1");
        assert_eq!(env.payload_tag, "Greeting");
    }

    #[test]
    fn self_closing_payload() {
        let xml = br#"<message xmlns="https://xml-pipeline.org/ns/envelope/v1">
  <meta>
    <from>system</from>
    <thread>t1</thread>
  </meta>
  <Ping/>
</message>"#;
        let env = parse_envelope(xml).unwrap();
        assert_eq!(env.payload_tag, "Ping");
    }

    #[test]
    fn garbage_bytes_rejected() {
        let err = parse_envelope(b"not xml at all").unwrap_err();
        assert!(matches!(err, PipelineError::EnvelopeParse(_)));
    }
}
