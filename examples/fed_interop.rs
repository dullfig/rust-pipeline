//! Cross-language interop harness for the federation wire protocol.
//!
//! Builds a fixed envelope and exposes three subcommands so `python/interop_test.py` can
//! seal on one side and open on the other, both directions:
//!   - `expected`     → print the canonical summary of the fixed envelope
//!   - `seal`         → print hex of a sealed frame (for Python to open)
//!   - `open <hex>`   → open a hex frame (e.g. sealed by Python) and print its summary

use rust_pipeline::prelude::*;

const KEY: PeerKey = [7u8; 32];

fn fixed_env() -> Envelope {
    Envelope {
        meta: Meta {
            from: Address::parse("ringhub.bob").unwrap(),
            to: Some(Address::parse("agentos.concierge[alice]").unwrap()),
            thread: "t-interop".into(),
            provenance: Provenance::from_bit(3),
        },
        payload: Payload {
            tag: "Order".into(),
            value: PayloadValue::Rec(vec![
                Field::new("item", PayloadValue::Text("two coffees".into())),
                Field::new("qty", PayloadValue::Uint(2)),
            ]),
        },
    }
}

fn summary(env: &Envelope) -> String {
    let item = env
        .payload
        .value
        .get("item")
        .and_then(|v| v.as_text())
        .unwrap_or("?");
    let qty = match env.payload.value.get("qty") {
        Some(PayloadValue::Uint(n)) => n.to_string(),
        _ => "?".into(),
    };
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        env.meta.from,
        env.meta.to.as_ref().map(|a| a.to_string()).unwrap_or_default(),
        env.meta.thread,
        env.meta.provenance.bits_lo,
        env.payload.tag,
        item,
        qty
    )
}

fn directory() -> PeerDirectory {
    let mut d = PeerDirectory::new();
    d.register(Peer {
        namespace: "ringhub".into(),
        endpoint: "x".into(),
        key: KEY,
        inbound_provenance: Provenance::EMPTY,
    });
    d
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("expected") => println!("{}", summary(&fixed_env())),
        Some("seal") => {
            let frame = seal(&fixed_env(), "ringhub", &KEY).unwrap();
            println!("{}", to_hex(&frame));
        }
        Some("open") => {
            let frame = from_hex(&args[2]);
            let (env, sender) = open(&frame, &directory()).expect("open failed");
            assert_eq!(sender, "ringhub", "authenticated sender mismatch");
            println!("{}", summary(&env));
        }
        _ => {
            eprintln!("usage: fed_interop expected | seal | open <hex>");
            std::process::exit(2);
        }
    }
}
