//! XSD-style validation for pipeline messages.
//!
//! In xml-pipeline, payloads are validated against XSD schemas generated
//! from `@xmlify` dataclasses. Since we control schema generation in
//! AgentOS, we implement structural validation directly rather than
//! depending on a full XSD library.
//!
//! The validation stage sits between parsing and routing — it's the
//! second gate a message must pass through to earn trust.
//!
//! # Approach
//!
//! Schemas are represented as `PayloadSchema` structs that describe
//! expected element structure. Validation checks:
//! - Root element tag matches expected tag
//! - Required child elements are present
//! - No unexpected elements (strict mode) or extras allowed (lax mode)
//!
//! This maps to what the Python version does: the XSD is generated
//! from code, so we know the exact structure we expect.

use std::collections::HashMap;

use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::error::{PipelineError, PipelineResult};

/// Schema for a payload element.
///
/// Describes the expected structure of a payload XML element.
/// Generated from handler registration (like Python's `@xmlify`).
#[derive(Debug, Clone, PartialEq)]
pub struct PayloadSchema {
    /// Expected root element tag name.
    pub root_tag: String,
    /// Expected child elements. Key = tag name, Value = field schema.
    pub fields: HashMap<String, FieldSchema>,
    /// Whether to reject unexpected child elements.
    pub strict: bool,
}

/// Schema for a single field in a payload.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSchema {
    /// Whether this field must be present.
    pub required: bool,
    /// Expected type (for documentation; validation checks presence only).
    pub field_type: FieldType,
}

/// Field types (for documentation and future type checking).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Boolean,
    /// Nested element with its own schema.
    Complex(Box<PayloadSchema>),
    /// Any content allowed (lax).
    Any,
}

/// Validate a payload's XML bytes against a schema.
///
/// Returns `Ok(())` if the payload matches the schema, or a
/// `PipelineError::Validation` with a description of what failed.
pub fn validate_payload(payload_xml: &[u8], schema: &PayloadSchema) -> PipelineResult<()> {
    let mut reader = Reader::from_reader(payload_xml);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut found_root = false;
    let mut found_fields: HashMap<String, bool> = HashMap::new();
    let mut depth: u32 = 0;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = local_name(e.name().as_ref());
                depth += 1;

                if depth == 1 {
                    // Root element
                    if tag != schema.root_tag {
                        return Err(PipelineError::Validation(format!(
                            "expected root <{}>, found <{tag}>",
                            schema.root_tag
                        )));
                    }
                    found_root = true;
                } else if depth == 2 && found_root {
                    // Direct children of root
                    if schema.strict && !schema.fields.contains_key(&tag) {
                        return Err(PipelineError::Validation(format!(
                            "unexpected element <{tag}> in <{}>",
                            schema.root_tag
                        )));
                    }
                    found_fields.insert(tag.to_string(), true);
                }
                // Deeper nesting: skip (we don't validate nested structure in Phase 1)
            }
            Ok(Event::Empty(ref e)) => {
                let tag = local_name(e.name().as_ref());
                depth += 1;

                if depth == 1 {
                    // Self-closing root
                    if tag != schema.root_tag {
                        return Err(PipelineError::Validation(format!(
                            "expected root <{}>, found <{tag}/>",
                            schema.root_tag
                        )));
                    }
                    found_root = true;
                } else if depth == 2 && found_root {
                    if schema.strict && !schema.fields.contains_key(&tag) {
                        return Err(PipelineError::Validation(format!(
                            "unexpected element <{tag}/> in <{}>",
                            schema.root_tag
                        )));
                    }
                    found_fields.insert(tag.to_string(), true);
                }

                depth -= 1;
            }
            Ok(Event::End(_)) => {
                depth -= 1;
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(PipelineError::Validation(format!(
                    "XML parse error during validation: {e}"
                )))
            }
            _ => {}
        }
        buf.clear();
    }

    if !found_root {
        return Err(PipelineError::Validation(
            "no root element found in payload".into(),
        ));
    }

    // Check required fields
    for (field_name, field_schema) in &schema.fields {
        if field_schema.required && !found_fields.contains_key(field_name) {
            return Err(PipelineError::Validation(format!(
                "missing required element <{field_name}> in <{}>",
                schema.root_tag
            )));
        }
    }

    Ok(())
}

/// Build a permissive schema that accepts any payload with the given root tag.
///
/// Used for handlers that don't declare a specific schema.
pub fn permissive_schema(root_tag: &str) -> PayloadSchema {
    PayloadSchema {
        root_tag: root_tag.to_string(),
        fields: HashMap::new(),
        strict: false,
    }
}

/// Registry of schemas for known payload types.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas: HashMap<String, PayloadSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a schema for a payload tag.
    pub fn register(&mut self, schema: PayloadSchema) {
        self.schemas.insert(schema.root_tag.clone(), schema);
    }

    /// Look up a schema by payload tag.
    pub fn get(&self, tag: &str) -> Option<&PayloadSchema> {
        self.schemas.get(tag)
    }

    /// Validate a payload against its registered schema.
    ///
    /// If no schema is registered for this tag, uses a permissive schema
    /// (accepts any structure with the correct root tag).
    pub fn validate(&self, tag: &str, payload_xml: &[u8]) -> PipelineResult<()> {
        match self.schemas.get(tag) {
            Some(schema) => validate_payload(payload_xml, schema),
            None => {
                // No schema registered — use permissive validation
                // (just check the root tag exists)
                let schema = permissive_schema(tag);
                validate_payload(payload_xml, &schema)
            }
        }
    }
}

/// Extract local name from a possibly-namespaced tag.
fn local_name(name: &[u8]) -> String {
    let s = std::str::from_utf8(name).unwrap_or("");
    if let Some(pos) = s.rfind(':') {
        s[pos + 1..].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn greeting_schema() -> PayloadSchema {
        let mut fields = HashMap::new();
        fields.insert(
            "text".into(),
            FieldSchema {
                required: true,
                field_type: FieldType::String,
            },
        );
        fields.insert(
            "name".into(),
            FieldSchema {
                required: false,
                field_type: FieldType::String,
            },
        );

        PayloadSchema {
            root_tag: "Greeting".into(),
            fields,
            strict: true,
        }
    }

    #[test]
    fn valid_payload() {
        let xml = b"<Greeting><text>Hello!</text></Greeting>";
        validate_payload(xml, &greeting_schema()).unwrap();
    }

    #[test]
    fn valid_with_optional() {
        let xml = b"<Greeting><text>Hello!</text><name>Alice</name></Greeting>";
        validate_payload(xml, &greeting_schema()).unwrap();
    }

    #[test]
    fn missing_required_field() {
        let xml = b"<Greeting><name>Alice</name></Greeting>";
        let err = validate_payload(xml, &greeting_schema()).unwrap_err();
        assert!(err.to_string().contains("missing required"));
        assert!(err.to_string().contains("text"));
    }

    #[test]
    fn wrong_root_tag() {
        let xml = b"<WrongTag><text>hi</text></WrongTag>";
        let err = validate_payload(xml, &greeting_schema()).unwrap_err();
        assert!(err.to_string().contains("expected root"));
    }

    #[test]
    fn unexpected_element_strict() {
        let xml = b"<Greeting><text>hi</text><secret>data</secret></Greeting>";
        let err = validate_payload(xml, &greeting_schema()).unwrap_err();
        assert!(err.to_string().contains("unexpected element"));
    }

    #[test]
    fn unexpected_element_lax() {
        let mut schema = greeting_schema();
        schema.strict = false;
        let xml = b"<Greeting><text>hi</text><extra>ok</extra></Greeting>";
        validate_payload(xml, &schema).unwrap(); // lax mode allows extras
    }

    #[test]
    fn permissive_schema_accepts_anything() {
        let schema = permissive_schema("Payload");
        let xml = b"<Payload><anything>goes</anything><here>too</here></Payload>";
        validate_payload(xml, &schema).unwrap();
    }

    #[test]
    fn schema_registry_lookup() {
        let mut reg = SchemaRegistry::new();
        reg.register(greeting_schema());

        // Known schema — validated
        let xml = b"<Greeting><text>hi</text></Greeting>";
        reg.validate("Greeting", xml).unwrap();

        // Unknown schema — permissive
        let xml2 = b"<Unknown><anything>ok</anything></Unknown>";
        reg.validate("Unknown", xml2).unwrap();
    }
}
