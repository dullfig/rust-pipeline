"""
Cross-language interop test: seal in one language, open in the other, both directions.
Run from this directory:  python interop_test.py
(Builds the Rust `fed_interop` example via cargo; requires the rust-pipeline crate alongside.)
"""
import os
import subprocess
import sys

import rust_pipeline_federation as fed

KEY = bytes([7]) * 32
_RP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fixed_env() -> fed.Envelope:
    return fed.Envelope(
        fed.Meta(
            from_=fed.Address.parse("ringhub.bob"),
            to=fed.Address.parse("agentos.concierge[alice]"),
            thread="t-interop",
            provenance=(1 << 3),
        ),
        fed.Payload(
            "Order",
            fed.PayloadValue.rec([
                ("item", fed.PayloadValue.text("two coffees")),
                ("qty", fed.PayloadValue.uint(2)),
            ]),
        ),
    )


def summary(env: fed.Envelope) -> str:
    item = env.payload.value.get("item")
    item = item.as_text() if item else "?"
    qtyv = env.payload.value.get("qty")
    qty = str(qtyv.value) if qtyv and qtyv.kind == "uint" else "?"
    prov_lo = env.meta.provenance & ((1 << 64) - 1)
    to = str(env.meta.to) if env.meta.to else ""
    return f"{env.meta.from_}|{to}|{env.meta.thread}|{prov_lo}|{env.payload.tag}|{item}|{qty}"


def rust(*a: str) -> str:
    r = subprocess.run(
        ["cargo", "run", "--quiet", "--example", "fed_interop", "--", *a],
        cwd=_RP_DIR, capture_output=True, text=True,
    )
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        raise SystemExit(f"rust example failed: {a}")
    return r.stdout.strip()


def main() -> None:
    e = fixed_env()
    py_exp = summary(e)

    # 0. Same envelope representation across languages.
    rust_exp = rust("expected")
    assert rust_exp == py_exp, f"representation mismatch:\n rust={rust_exp}\n py  ={py_exp}"

    # 1. Rust seals → Python opens (Rust→Python: crypto + XML decode).
    rust_hex = rust("seal")
    d = fed.PeerDirectory()
    d.register(fed.Peer("ringhub", "x", KEY))
    env_from_rust, sender = fed.open_frame(bytes.fromhex(rust_hex), d)
    assert sender == "ringhub", f"authenticated sender = {sender!r}"
    assert summary(env_from_rust) == py_exp, f"rust→py: {summary(env_from_rust)} != {py_exp}"

    # 2. Python seals → Rust opens (Python→Rust: crypto + XML decode).
    py_frame = fed.seal(e, "ringhub", KEY)
    rust_summary = rust("open", py_frame.hex())
    assert rust_summary == py_exp, f"py→rust: {rust_summary} != {py_exp}"

    # 3. Tamper → fail-closed on the Python side too.
    bad = bytearray(rust_hex_bytes := bytes.fromhex(rust_hex))
    bad[-1] ^= 0xFF
    try:
        fed.open_frame(bytes(bad), d)
        raise SystemExit("FAIL: tampered frame opened")
    except ValueError:
        pass

    print("CROSS-LANGUAGE INTEROP OK")
    print("  envelope:", py_exp)
    print("  rust->py, py->rust, and tamper-rejection all verified.")


if __name__ == "__main__":
    main()
