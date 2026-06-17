"""
Standalone self-test for rust_pipeline_federation — no Rust/cargo needed.

Verifies:
  1. Python seal -> open round-trip.
  2. Python opens a FROZEN, real rust-pipeline-sealed frame to the expected envelope
     (proves cross-language XChaCha20-Poly1305 + XML decode without needing Rust here).
  3. wrong-key and tampered frames fail closed.

Run:  python selftest.py   (requires: pip install pynacl)
"""
import rust_pipeline_federation as fed

# Fixed key matching the frozen vector below (rust-pipeline's fed_interop uses [7u8;32]).
KEY = bytes([7]) * 32

# A real frame produced by `cargo run --example fed_interop -- seal` (sender "ringhub").
# Header `01 00 0007 "ringhub"` ‖ nonce(24) ‖ XChaCha20Poly1305 ciphertext+tag.
FROZEN_RUST_FRAME = bytes.fromhex(
    "0100000772696e676875620b30f5e37f4caf7987f3773b9796c8960353702e8912eb4f19f9"
    "8dac29e140bbc9dc17cd1cc532bff665ddf682f38684fb7bffbb4a741a9f6abecd2e20d5a93"
    "8634a29c02ec8a2da866a0b23a08ea0938d266a1d6b67789236150274dfd98834332b6309c9"
    "ec2769dc49b4c24be47121294f45ecb5d358994410e5f7f8fe38fb6b6f258ecfb4f02e760e5"
    "b8accdddb3812683aca6c9b6d561c4cdc2bc3f375307d53a17f09925364241489ace40d2062"
    "4550e9511ed7f64db44ca9a488130fe8df2f075a22906a48daa9012938e376b3df5570229d0"
    "5c34572a108e317e09ca7cbb3b7527d06030436110f8f574fac98c6482a3c7d88c37fc261f2"
    "4f10b461e140f9db56fbff669a142f6945f8fb860b790afeb5c105191094a0f4dfc4bdff337"
    "b35b2eb5f69ecc43378535fcd97a0cfd313af0e66880768db95c144448d087c3ee2f1bb23c1"
    "9f83123c8bb38822f4dfd4e4c3e7c1d37f7cca0a5cdc9758ae269edd3097d0221bf194cb3bb"
    "61d573eb95b6fb848a4bb31eb44561e85a6"
)

EXPECTED = "ringhub.bob|agentos.concierge[alice]|t-interop|8|Order|two coffees|2"


def summary(env: fed.Envelope) -> str:
    item = env.payload.value.get("item")
    item = item.as_text() if item else "?"
    qtyv = env.payload.value.get("qty")
    qty = str(qtyv.value) if qtyv and qtyv.kind == "uint" else "?"
    prov_lo = env.meta.provenance & ((1 << 64) - 1)
    to = str(env.meta.to) if env.meta.to else ""
    return f"{env.meta.from_}|{to}|{env.meta.thread}|{prov_lo}|{env.payload.tag}|{item}|{qty}"


def expected_env() -> fed.Envelope:
    return fed.Envelope(
        fed.Meta(
            from_=fed.Address.parse("ringhub.bob"),
            to=fed.Address.parse("agentos.concierge[alice]"),
            thread="t-interop",
            provenance=(1 << 3),
        ),
        fed.Payload("Order", fed.PayloadValue.rec([
            ("item", fed.PayloadValue.text("two coffees")),
            ("qty", fed.PayloadValue.uint(2)),
        ])),
    )


def main() -> None:
    d = fed.PeerDirectory()
    d.register(fed.Peer("ringhub", "x", KEY))

    # 1. Python round-trip.
    frame = fed.seal(expected_env(), "ringhub", KEY)
    back, sender = fed.open_frame(frame, d)
    assert sender == "ringhub"
    assert summary(back) == EXPECTED, summary(back)

    # 2. Open a real Rust-sealed frame.
    rust_env, rs = fed.open_frame(FROZEN_RUST_FRAME, d)
    assert rs == "ringhub"
    assert summary(rust_env) == EXPECTED, summary(rust_env)

    # 3. Fail-closed: wrong key.
    d_bad = fed.PeerDirectory()
    d_bad.register(fed.Peer("ringhub", "x", bytes([9]) * 32))
    try:
        fed.open_frame(FROZEN_RUST_FRAME, d_bad)
        raise SystemExit("FAIL: wrong key opened")
    except ValueError:
        pass

    # 3b. Fail-closed: tampered frame.
    bad = bytearray(FROZEN_RUST_FRAME)
    bad[-1] ^= 0xFF
    try:
        fed.open_frame(bytes(bad), d)
        raise SystemExit("FAIL: tampered frame opened")
    except ValueError:
        pass

    print("SELFTEST OK")
    print("  Python round-trip, frozen real Rust frame, wrong-key + tamper rejection.")


if __name__ == "__main__":
    main()
