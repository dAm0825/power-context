# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Canonical context packs and prefix-reusing transport deltas."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any, NoReturn

PACK_SCHEMA = "powercontext.context-pack.v1"
DELTA_SCHEMA = "powercontext.context-pack-delta.v1"


class ContextPackError(ValueError):
    """A ContextPack or delta violates the wire contract."""


@dataclass(frozen=True, slots=True)
class ContextPack:
    """An immutable sequence of UTF-8-safe context chunks."""

    items: tuple[bytes, ...]

    def __post_init__(self) -> None:
        items = tuple(self.items)
        if any(not isinstance(item, bytes) for item in items):
            raise ContextPackError("context pack items must be bytes")
        _validate_utf8_chunks(items)
        object.__setattr__(self, "items", items)

    @classmethod
    def build(cls, items: tuple[bytes, ...], /) -> ContextPack:
        return cls(tuple(items))

    @classmethod
    def from_content(cls, content: bytes | str, *, chunk_bytes: int = 512) -> ContextPack:
        if isinstance(content, str):
            raw = content.encode("utf-8")
        elif isinstance(content, bytes):
            raw = content
        else:
            raise ContextPackError("content must be bytes or text")
        if not isinstance(chunk_bytes, int) or isinstance(chunk_bytes, bool) or chunk_bytes <= 0:
            raise ContextPackError("chunk_bytes must be a positive integer")
        if not raw:
            return cls(())
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ContextPackError("content must be valid UTF-8") from error

        chunks: list[bytes] = []
        current = bytearray()
        for character in text:
            encoded = character.encode("utf-8")
            if len(encoded) > chunk_bytes:
                raise ContextPackError("one UTF-8 character exceeds the chunk byte limit")
            if current and len(current) + len(encoded) > chunk_bytes:
                chunks.append(bytes(current))
                current.clear()
            current.extend(encoded)
        if current:
            chunks.append(bytes(current))
        return cls.build(tuple(chunks))

    @classmethod
    def from_bytes(cls, value: bytes, /) -> ContextPack:
        if not isinstance(value, bytes):
            raise ContextPackError("serialized context pack must be bytes")
        try:
            payload = json.loads(value.decode("utf-8"), object_pairs_hook=_unique_object)
        except (UnicodeError, ValueError, TypeError) as error:
            raise ContextPackError("serialized context pack must be valid JSON") from error
        if not isinstance(payload, dict) or set(payload) != {"schema", "items", "digest", "byte_count"}:
            raise ContextPackError("context pack has missing or extra fields")
        if payload["schema"] != PACK_SCHEMA:
            raise ContextPackError("unsupported context pack schema")
        items = _decode_items(payload["items"])
        pack = cls.build(items)
        if payload["digest"] != pack.digest:
            raise ContextPackError("context pack digest does not match its items")
        byte_count = payload["byte_count"]
        if (
            isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count != len(value)
            or byte_count != pack.byte_count
        ):
            raise ContextPackError("context pack byte_count does not match its serialization")
        return pack

    @property
    def digest(self) -> str:
        return _pack_digest(self.items)

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        base_payload = {
            "digest": self.digest,
            "items": _encode_items(self.items),
            "schema": PACK_SCHEMA,
        }
        return _serialize_pack(base_payload)


@dataclass(frozen=True, slots=True)
class ContextPackDelta:
    """A target suffix paired with an existing target-pack prefix."""

    base_digest: str
    target_digest: str
    prefix_items: int
    appended: tuple[bytes, ...]

    def __post_init__(self) -> None:
        if not _is_digest(self.base_digest) or not _is_digest(self.target_digest):
            raise ContextPackError("context pack delta digests are invalid")
        if isinstance(self.prefix_items, bool) or not isinstance(self.prefix_items, int) or self.prefix_items < 0:
            raise ContextPackError("prefix_items must be a non-negative integer")
        appended = tuple(self.appended)
        if any(not isinstance(item, bytes) for item in appended):
            raise ContextPackError("context pack delta chunks must be bytes")
        _validate_utf8_chunks(appended)
        object.__setattr__(self, "appended", appended)

    @classmethod
    def between(cls, base: ContextPack, target: ContextPack, /) -> ContextPackDelta:
        if not isinstance(base, ContextPack) or not isinstance(target, ContextPack):
            raise ContextPackError("delta endpoints must be context packs")
        prefix_items = 0
        for old, new in zip(base.items, target.items, strict=False):
            if old != new:
                break
            prefix_items += 1
        return cls(
            base_digest=base.digest,
            target_digest=target.digest,
            prefix_items=prefix_items,
            appended=target.items[prefix_items:],
        )

    @classmethod
    def from_bytes(cls, value: bytes, /) -> ContextPackDelta:
        if not isinstance(value, bytes):
            raise ContextPackError("serialized context pack delta must be bytes")
        try:
            payload = json.loads(value.decode("utf-8"), object_pairs_hook=_unique_object)
        except (UnicodeError, ValueError, TypeError) as error:
            raise ContextPackError("serialized context pack delta must be valid JSON") from error
        if not isinstance(payload, dict) or set(payload) != {
            "schema",
            "base_digest",
            "target_digest",
            "prefix_items",
            "appended",
        }:
            raise ContextPackError("context pack delta has missing or extra fields")
        if payload["schema"] != DELTA_SCHEMA:
            raise ContextPackError("unsupported context pack delta schema")
        base_digest = payload["base_digest"]
        target_digest = payload["target_digest"]
        if not _is_digest(base_digest) or not _is_digest(target_digest):
            raise ContextPackError("context pack delta digests are invalid")
        prefix_items = payload["prefix_items"]
        if (
            isinstance(prefix_items, bool)
            or not isinstance(prefix_items, int)
            or prefix_items < 0
        ):
            raise ContextPackError("prefix_items must be a non-negative integer")
        delta = cls(
            base_digest=base_digest,
            target_digest=target_digest,
            prefix_items=prefix_items,
            appended=_decode_items(payload["appended"]),
        )
        return delta

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        payload = {
            "appended": _encode_items(self.appended),
            "base_digest": self.base_digest,
            "prefix_items": self.prefix_items,
            "schema": DELTA_SCHEMA,
            "target_digest": self.target_digest,
        }
        return _canonical_json(payload)


def apply_context_pack_delta(base: ContextPack, delta: ContextPackDelta, /) -> ContextPack:
    """Validate a delta against its base and rebuild the complete target pack."""

    if not isinstance(base, ContextPack) or not isinstance(delta, ContextPackDelta):
        raise ContextPackError("base must be a pack and delta must be a context pack delta")
    if delta.base_digest != base.digest:
        raise ContextPackError("delta base digest does not match the supplied pack")
    if delta.prefix_items < 0 or delta.prefix_items > len(base.items):
        raise ContextPackError("delta prefix exceeds the supplied pack")
    target = ContextPack.build((*base.items[: delta.prefix_items], *delta.appended))
    if target.digest != delta.target_digest:
        raise ContextPackError("rebuilt context pack does not match the target digest")
    return target


def _pack_digest(items: tuple[bytes, ...]) -> str:
    unsigned = {"items": _encode_items(items), "schema": PACK_SCHEMA}
    return f"sha256:{hashlib.sha256(_canonical_json(unsigned)).hexdigest()}"


def _serialize_pack(base_payload: dict[str, Any]) -> bytes:
    byte_count = 0
    while True:
        payload = {**base_payload, "byte_count": byte_count}
        encoded = _canonical_json(payload)
        if len(encoded) == byte_count:
            return encoded
        byte_count = len(encoded)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _encode_items(items: tuple[bytes, ...]) -> list[str]:
    return [base64.b64encode(item).decode("ascii") for item in items]


def _decode_items(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContextPackError("chunk arrays must contain Base64 strings")
    decoded: list[bytes] = []
    for encoded in value:
        try:
            item = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ContextPackError("invalid Base64 context chunk") from error
        if base64.b64encode(item).decode("ascii") != encoded:
            raise ContextPackError("context chunks must use canonical Base64")
        decoded.append(item)
    _validate_utf8_chunks(tuple(decoded))
    return tuple(decoded)


def _validate_utf8_chunks(items: tuple[bytes, ...]) -> None:
    for item in items:
        try:
            item.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ContextPackError("context chunks must be valid UTF-8") from error


def _is_digest(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        return False
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError:
        return False
    return value == value.lower()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _raise_duplicate_key()
        result[key] = value
    return result


def _raise_duplicate_key() -> NoReturn:
    raise ContextPackError("duplicate JSON key")


__all__ = [
    "DELTA_SCHEMA",
    "PACK_SCHEMA",
    "ContextPack",
    "ContextPackDelta",
    "ContextPackError",
    "apply_context_pack_delta",
]
