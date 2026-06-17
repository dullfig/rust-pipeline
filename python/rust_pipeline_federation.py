"""
rust_pipeline_federation — Python reference implementation of the rust-pipeline
federation wire protocol, for RingHub (or any peer node) to exchange messages with an
AgentOS node.

Byte-for-byte interoperable with `rust-pipeline/src/federation.rs` + `src/codec.rs`
(verified by `python/interop_test.py`, which seals on one side and opens on the other in
both directions). Keep the two in sync.

Wire protocol
-------------
    header = version:u8(=1) ‖ auth_method:u8(=0 PSK) ‖ sender_len:u16(BE) ‖ sender:utf8
    frame  = header ‖ nonce:24 ‖ XChaCha20Poly1305(key, nonce, encode_envelope(env), aad=header)

- Crypto: XChaCha20-Poly1305 IETF (libsodium via PyNaCl) == RustCrypto XChaCha20Poly1305.
- The sender's node identity rides in the header, bound as AEAD associated data: `open`
  looks up that sender's key and returns the *authenticated* sender. `version`/`auth_method`
  reserve upgrades (e.g. ed25519 per-node signatures) without a reframe.
- Plaintext is the canonical envelope as XML (cross-language; XML for now, swappable later).

Requires: pynacl   (pip install pynacl)
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional, Union
from xml.sax.saxutils import escape as _xml_escape, quoteattr as _xml_quoteattr
import xml.etree.ElementTree as ET

from nacl.bindings import (
    crypto_aead_xchacha20poly1305_ietf_encrypt as _aead_encrypt,
    crypto_aead_xchacha20poly1305_ietf_decrypt as _aead_decrypt,
)
from nacl.utils import random as _random_bytes

ENVELOPE_NS = "https://xml-pipeline.org/ns/envelope/v1"
_VERSION = 1
_AUTH_PSK = 0
_NONCE_LEN = 24
_U64 = (1 << 64) - 1


# ── Address (mirrors wire::Address) ──────────────────────────────────────

@dataclass
class Segment:
    name: str
    key: Optional[str] = None

    def cache_keys(self) -> list[str]:
        return self.key.split("+") if self.key else []

    def __str__(self) -> str:
        return f"{self.name}[{self.key}]" if self.key is not None else self.name


@dataclass
class Address:
    segments: list[Segment]

    @staticmethod
    def parse(s: str) -> "Address":
        s = s.strip()
        if not s:
            raise ValueError("empty address")
        segs: list[Segment] = []
        name, key, in_bracket = "", None, False
        for ch in s:
            if ch == "[":
                if in_bracket:
                    raise ValueError("unclosed bracket")
                in_bracket, key = True, ""
            elif ch == "]":
                if not in_bracket:
                    raise ValueError("unexpected ]")
                in_bracket = False
            elif ch == "." and not in_bracket:
                if not name:
                    raise ValueError("empty segment name")
                segs.append(Segment(name, key))
                name, key = "", None
            else:
                if in_bracket:
                    key += ch
                else:
                    name += ch
        if in_bracket:
            raise ValueError("unclosed bracket")
        if name:
            segs.append(Segment(name, key))
        if not segs:
            raise ValueError("empty address")
        return Address(segs)

    @staticmethod
    def flat(name: str) -> "Address":
        return Address([Segment(name)])

    def __str__(self) -> str:
        return ".".join(str(s) for s in self.segments)

    def _org_idx(self) -> int:
        for i, s in enumerate(self.segments):
            if s.key is not None:
                return i
        return 0

    def organism(self) -> Optional[str]:
        return self.segments[self._org_idx()].name if self.segments else None

    def namespace(self) -> Optional[str]:
        return self.segments[0].name if self._org_idx() > 0 else None

    def node(self) -> Optional[str]:
        # The federation node = the leading segment (reliable regardless of organism keys).
        # Authz matrices key on this, not namespace(). On a re-rooted inbound `from` it is
        # the authenticated peer.
        return self.segments[0].name if self.segments else None

    def instance_key(self) -> Optional[str]:
        i = self._org_idx()
        return self.segments[i].key if i < len(self.segments) else None

    def buffer(self) -> Optional[Segment]:
        i = self._org_idx()
        return self.segments[i + 1] if len(self.segments) > i + 1 else None

    def cache_keys(self) -> list[str]:
        return self.segments[self._org_idx()].cache_keys() if self.segments else []

    def instance_address(self) -> "Address":
        return Address(self.segments[: self._org_idx() + 1])


# ── PayloadValue (mirrors the self-describing wire::PayloadValue) ─────────

class PayloadValue:
    """Tagged value. Internal `kind` matches the Rust enum (boolean wire-tag is 'bool')."""
    __slots__ = ("kind", "value")

    def __init__(self, kind: str, value):
        self.kind = kind
        self.value = value

    @staticmethod
    def rec(fields: Union[dict, list]) -> "PayloadValue":
        items = list(fields.items()) if isinstance(fields, dict) else list(fields)
        return PayloadValue("rec", items)  # ordered list[(name, PayloadValue)]

    @staticmethod
    def seq(items) -> "PayloadValue":
        return PayloadValue("seq", list(items))

    @staticmethod
    def text(s) -> "PayloadValue":
        return PayloadValue("text", str(s))

    @staticmethod
    def uint(n) -> "PayloadValue":
        return PayloadValue("uint", int(n))

    @staticmethod
    def sint(n) -> "PayloadValue":
        return PayloadValue("sint", int(n))

    @staticmethod
    def real(x) -> "PayloadValue":
        return PayloadValue("real", float(x))

    @staticmethod
    def boolean(b) -> "PayloadValue":
        return PayloadValue("boolean", bool(b))

    @staticmethod
    def blob(b) -> "PayloadValue":
        return PayloadValue("blob", bytes(b))

    @staticmethod
    def nil() -> "PayloadValue":
        return PayloadValue("nil", None)

    def get(self, name: str) -> Optional["PayloadValue"]:
        if self.kind == "rec":
            for n, v in self.value:
                if n == name:
                    return v
        return None

    def as_text(self) -> Optional[str]:
        return self.value if self.kind == "text" else None

    def __eq__(self, o) -> bool:
        return isinstance(o, PayloadValue) and self.kind == o.kind and self.value == o.value

    def __repr__(self) -> str:
        return f"PayloadValue({self.kind!r}, {self.value!r})"


@dataclass
class Payload:
    tag: str
    value: PayloadValue

    @staticmethod
    def single(tag: str, field_name: str, text: str) -> "Payload":
        return Payload(tag, PayloadValue.rec([(field_name, PayloadValue.text(text))]))


@dataclass
class Meta:
    from_: Address
    to: Optional[Address]
    thread: str
    provenance: int = 0  # 128-bit provenance set as a Python int (bit i ↔ provenance bit i)


@dataclass
class Envelope:
    meta: Meta
    payload: Payload


# ── Canonical envelope codec (XML — mutually parseable with codec.rs) ─────

def _encode_value(v: PayloadValue) -> str:
    k = v.kind
    if k == "rec":
        inner = "".join(
            f"<f n={_xml_quoteattr(n)}>{_encode_value(val)}</f>" for n, val in v.value
        )
        return f'<v k="rec">{inner}</v>'
    if k == "seq":
        return f'<v k="seq">{"".join(_encode_value(x) for x in v.value)}</v>'
    if k == "text":
        return f'<v k="text">{_xml_escape(v.value)}</v>'
    if k == "uint":
        return f'<v k="uint">{v.value}</v>'
    if k == "sint":
        return f'<v k="sint">{v.value}</v>'
    if k == "real":
        return f'<v k="real">{v.value!r}</v>'
    if k == "boolean":
        return f'<v k="bool">{"true" if v.value else "false"}</v>'
    if k == "blob":
        return f'<v k="blob">{v.value.hex()}</v>'
    if k == "nil":
        return '<v k="nil"/>'
    raise ValueError(f"unknown value kind: {k}")


def encode_envelope(env: Envelope) -> bytes:
    m = env.meta
    parts = [f'<message xmlns="{ENVELOPE_NS}"><meta>']
    parts.append(f"<from>{_xml_escape(str(m.from_))}</from>")
    if m.to is not None:
        parts.append(f"<to>{_xml_escape(str(m.to))}</to>")
    parts.append(f"<thread>{_xml_escape(m.thread)}</thread>")
    if m.provenance:
        hi, lo = (m.provenance >> 64) & _U64, m.provenance & _U64
        parts.append(f"<provenance>{hi:016x}{lo:016x}</provenance>")
    parts.append("</meta>")
    parts.append(f"<payload tag={_xml_quoteattr(env.payload.tag)}>")
    parts.append(_encode_value(env.payload.value))
    parts.append("</payload></message>")
    return "".join(parts).encode("utf-8")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _decode_value(el: ET.Element) -> PayloadValue:
    k = el.get("k")
    if k == "rec":
        items = []
        for f in el:
            if _local(f.tag) != "f":
                continue
            child = next((c for c in f if _local(c.tag) == "v"), None)
            items.append((f.get("n"), _decode_value(child) if child is not None else PayloadValue.nil()))
        return PayloadValue("rec", items)
    if k == "seq":
        return PayloadValue("seq", [_decode_value(c) for c in el if _local(c.tag) == "v"])
    if k == "text":
        return PayloadValue.text(el.text or "")
    if k == "uint":
        return PayloadValue.uint(int(el.text or "0"))
    if k == "sint":
        return PayloadValue.sint(int(el.text or "0"))
    if k == "real":
        return PayloadValue.real(float(el.text or "0"))
    if k == "bool":
        return PayloadValue.boolean((el.text or "") == "true")
    if k == "blob":
        return PayloadValue.blob(bytes.fromhex(el.text or ""))
    if k == "nil":
        return PayloadValue.nil()
    raise ValueError(f"unknown value kind: {k}")


def decode_envelope(raw: bytes) -> Envelope:
    root = ET.fromstring(raw)
    if _local(root.tag) != "message":
        raise ValueError("not a <message>")
    meta_el = next(c for c in root if _local(c.tag) == "meta")
    from_ = to = thread = None
    prov = 0
    for c in meta_el:
        t = _local(c.tag)
        if t == "from":
            from_ = Address.parse(c.text)
        elif t == "to":
            to = Address.parse(c.text)
        elif t == "thread":
            thread = c.text
        elif t == "provenance":
            h = (c.text or "").strip()
            prov = (int(h[:16], 16) << 64) | int(h[16:], 16)
    payload_el = next(c for c in root if _local(c.tag) == "payload")
    v_el = next((c for c in payload_el if _local(c.tag) == "v"), None)
    value = _decode_value(v_el) if v_el is not None else PayloadValue.nil()
    if from_ is None or thread is None:
        raise ValueError("missing required meta")
    return Envelope(Meta(from_, to, thread, prov), Payload(payload_el.get("tag"), value))


# ── Sealed-frame protocol ────────────────────────────────────────────────

def _encode_header(sender: str) -> bytes:
    s = sender.encode("utf-8")
    return struct.pack(">BBH", _VERSION, _AUTH_PSK, len(s)) + s


def seal(envelope: Envelope, sender: str, key: bytes) -> bytes:
    """Seal `envelope` from this node `sender` to a peer holding `key` (32 bytes)."""
    if len(key) != 32:
        raise ValueError("key must be 32 bytes")
    header = _encode_header(sender)
    nonce = _random_bytes(_NONCE_LEN)
    ct = _aead_encrypt(encode_envelope(envelope), header, nonce, key)
    return header + nonce + ct


def open_frame(frame: bytes, directory: "PeerDirectory") -> tuple[Envelope, str]:
    """Open a sealed frame, authenticating the sender against `directory`.

    Returns (envelope, authenticated_sender). Raises on unknown sender, wrong key, or any
    tampering (fail-closed).
    """
    if len(frame) < 4:
        raise ValueError("malformed frame")
    version, auth = frame[0], frame[1]
    if version != _VERSION:
        raise ValueError(f"unsupported protocol version: {version}")
    if auth != _AUTH_PSK:
        raise ValueError(f"unsupported auth method: {auth}")
    (sender_len,) = struct.unpack(">H", frame[2:4])
    end = 4 + sender_len
    if len(frame) < end:
        raise ValueError("malformed frame")
    sender = frame[4:end].decode("utf-8")
    header = frame[:end]

    peer = directory.get(sender)
    if peer is None:
        raise ValueError(f"unknown peer: {sender}")

    body = frame[end:]
    if len(body) < _NONCE_LEN:
        raise ValueError("malformed frame")
    nonce, ct = body[:_NONCE_LEN], body[_NONCE_LEN:]
    try:
        plaintext = _aead_decrypt(ct, header, nonce, peer.key)
    except Exception as e:  # noqa: BLE001 — any AEAD failure is fail-closed
        raise ValueError("decrypt/authentication failed") from e
    return decode_envelope(plaintext), sender


# ── Peer directory + thin server ─────────────────────────────────────────

@dataclass
class Peer:
    namespace: str
    endpoint: str
    key: bytes
    inbound_provenance: int = 0  # unioned onto inbound envelopes (edge stamp)


class PeerDirectory:
    def __init__(self):
        self._peers: dict[str, Peer] = {}

    def register(self, peer: Peer) -> None:
        self._peers[peer.namespace] = peer

    def get(self, namespace: str) -> Optional[Peer]:
        return self._peers.get(namespace)

    def is_remote(self, namespace: str) -> bool:
        return namespace in self._peers


def _reroot_origin(from_addr: Address, namespace: str) -> Address:
    """Overwrite `from`'s leading namespace with the authenticated peer `namespace`.

    Makes origin unforgeable: a peer names which of its agents sent the message, never
    which namespace. Mirrors rust-pipeline's reroot_origin. Convention: leading segment is
    the namespace/node, so >=2 segments replace the claimed namespace; a bare agent is
    prefixed.
    """
    head = Segment(namespace)
    rest = from_addr.segments[1:] if len(from_addr.segments) >= 2 else from_addr.segments
    return Address([head, *rest])


class FederationServer:
    """Thin node-boundary server. Inject your transport (send sealed bytes to a peer
    endpoint), local delivery (hand an opened envelope to your app — e.g. Django), and an
    optional authorize(from, to) callable (the `from x to` matrix; None = allow all).

        server = FederationServer("ringhub", directory, my_send, my_handle, my_authorize)
        server.send(env)        # outbound: leading address segment = peer node -> seal+transmit
        server.receive(frame)   # inbound: open -> re-root origin -> stamp provenance ->
                                #          authorize -> strip self-ns -> deliver
    """

    def __init__(self, node: str, directory: PeerDirectory, transport_send, local_deliver,
                 authorize=None):
        self.node = node
        self.directory = directory
        self._send = transport_send      # (endpoint: str, frame: bytes) -> None
        self._deliver = local_deliver    # (envelope: Envelope) -> None
        self._authorize = authorize      # (from: Address, to: Address) -> bool; None = allow all

    def send(self, envelope: Envelope) -> None:
        to = envelope.meta.to
        if to is None or not to.segments:
            raise ValueError("envelope has no destination address")
        node = to.segments[0].name
        peer = self.directory.get(node)
        if peer is None:
            raise ValueError(f"not a remote node: {node}")
        self._send(peer.endpoint, seal(envelope, self.node, peer.key))

    def receive(self, frame: bytes) -> None:
        env, sender = open_frame(frame, self.directory)

        # Unforgeable origin: re-root `from` to the authenticated peer (overwrites any
        # namespace the sender claimed — they can never assert another node's namespace).
        env.meta.from_ = _reroot_origin(env.meta.from_, sender)

        peer = self.directory.get(sender)
        if peer is not None and peer.inbound_provenance:
            env.meta.provenance |= peer.inbound_provenance

        # Authorize from x to (re-homed check_namespace), before stripping. Fail-closed.
        if self._authorize is not None and env.meta.to is not None:
            if not self._authorize(env.meta.from_, env.meta.to):
                raise ValueError(f"unauthorized: {env.meta.from_} -> {env.meta.to}")

        to = env.meta.to
        if to is not None and len(to.segments) > 1 and to.segments[0].name == self.node:
            to.segments = to.segments[1:]  # strip our own node prefix -> locally routable
        self._deliver(env)
