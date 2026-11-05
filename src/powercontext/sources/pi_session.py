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

"""Pi v3 session history as exact, branch-scoped Source evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    AdapterSourceDefinition,
    Connector,
    ConnectorRunCompletion,
    ConnectorRunSession,
    ConnectorRunStatus,
    Source,
    SourceMaterialization,
    TextEvidence,
)

PI_EVENT_SOURCE_NAME = "pi-event"
PI_SESSION_CONNECTOR_NAME = "pi-session-history"
PI_SESSION_CONNECTOR_VERSION = "1"
_CHECKPOINT_SCHEMA = "powercontext.pi-session-checkpoint.v1"


class PiSessionInput(BaseModel):
    """A complete Pi v3 JSONL session and an optional selected leaf."""

    model_config = ConfigDict(extra="forbid")

    content: bytes
    leaf_id: str | None = None

    @field_validator("leaf_id")
    @classmethod
    def validate_leaf_id(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("leaf_id must be non-empty when provided")
        return value


class PiEventInput(BaseModel):
    """One visible event on the selected Pi history path."""

    model_config = ConfigDict(extra="forbid")

    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]

    @field_validator("session_id", "event_id", "event_type", "role", "content")
    @classmethod
    def validate_non_empty_text(cls, value: str) -> str:
        if not value:
            raise ValueError("field must be non-empty")
        return value

    @field_validator("parent_id")
    @classmethod
    def validate_parent_id(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("parent_id must be non-empty when present")
        return value


class PiEventSource(Source):
    """An immutable visible Pi event retaining its complete input record."""

    model_config = ConfigDict(extra="forbid")

    materialization: Literal[SourceMaterialization.CAPTURED] = SourceMaterialization.CAPTURED
    source_id: str = Field(max_length=MAX_SOURCE_ID_LENGTH)
    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("source_id must be a non-empty trimmed string")
        return value

    @field_validator("session_id", "event_id", "event_type", "role", "content")
    @classmethod
    def validate_non_empty_text(cls, value: str) -> str:
        if not value:
            raise ValueError("field must be non-empty")
        return value

    @field_validator("parent_id")
    @classmethod
    def validate_parent_id(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("parent_id must be non-empty when present")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> PiEventSource:
        if self.name != self.source_id:
            raise ValueError("name and source_id must be identical")
        expected = pi_event_source_id(self.session_id, self.event_id)
        if self.source_id != expected:
            raise ValueError("source_id must be derived from session_id and event_id")
        return self


class PiEventTextEvidenceProjection:
    """Expose one complete visible event through standard text evidence."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PiEventSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        return TextEvidence(
            source_type=PI_EVENT_SOURCE_NAME,
            source_id=source.source_id,
            content=source.content,
            metadata={
                "session_id": source.session_id,
                "event_id": source.event_id,
                "parent_id": source.parent_id,
                "event_type": source.event_type,
                "role": source.role,
            },
        )


class PiEventSourceAdapter:
    """Resolve visible Pi events into Sources and read them back losslessly."""

    input_class = PiEventInput
    name = PI_EVENT_SOURCE_NAME
    source_class = PiEventSource

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        source_id = pi_event_source_id(value.session_id, value.event_id)
        return PiEventSource(
            name=source_id,
            source_id=source_id,
            session_id=value.session_id,
            event_id=value.event_id,
            parent_id=value.parent_id,
            event_type=value.event_type,
            role=value.role,
            content=value.content,
            record=value.record,
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return PiEventInput(
            session_id=source.session_id,
            event_id=source.event_id,
            parent_id=source.parent_id,
            event_type=source.event_type,
            role=source.role,
            content=source.content,
            record=source.record,
        )


@dataclass(frozen=True, slots=True)
class _PiArchive:
    session_id: str
    records: tuple[dict[str, JsonValue], ...]
    by_id: dict[str, dict[str, JsonValue]]


class PiSessionConnector:
    """Incrementally ingest one selected branch while retaining all prior evidence."""

    name = PI_SESSION_CONNECTOR_NAME
    version = PI_SESSION_CONNECTOR_VERSION
    source_definitions = frozenset({PI_EVENT_SOURCE_NAME})

    def __init__(self, path: str | Path, leaf_id: str | None = None) -> None:
        self.path = Path(path)
        self.leaf_id = leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        archive = _parse_archive(self.path.read_bytes())
        selected = _selected_events(archive, self.leaf_id)
        imported = _load_checkpoint(session.checkpoint, archive.session_id)
        current = _archive_record_digests(archive)
        for source_id, expected_digest in imported.items():
            if current.get(source_id) != expected_digest:
                raise ValueError(f"previously imported Pi event changed or disappeared: {source_id}")

        next_imported = dict(imported)
        submitted = False
        for event in selected:
            source_id = pi_event_source_id(event.session_id, event.event_id)
            digest = _record_digest(event.record)
            if source_id in imported:
                continue
            outcome = await session.submit(event.event_id, PI_EVENT_SOURCE_NAME, event)
            if outcome.source_ref is not None and outcome.source_ref.source_id == source_id:
                next_imported[source_id] = digest
                submitted = True

        checkpoint: JsonValue | None
        if submitted:
            checkpoint = {
                "schema": _CHECKPOINT_SCHEMA,
                "session_id": archive.session_id,
                "events": next_imported,
            }
        else:
            checkpoint = session.checkpoint
        return ConnectorRunCompletion(status=ConnectorRunStatus.COMPLETE, checkpoint=checkpoint)


def parse_pi_session(value: PiSessionInput, /) -> tuple[PiEventInput, ...]:
    """Parse one selected Pi branch into visible events ordered from root to leaf."""

    archive = _parse_archive(value.content)
    return _selected_events(archive, value.leaf_id)


def pi_event_source_id(session_id: str, event_id: str, /) -> str:
    """Build a collision-safe identity from the session and event identifiers."""

    if not session_id or not event_id:
        raise ValueError("session_id and event_id must be non-empty")
    source_id = f"pi:{quote(session_id, safe='')}:{quote(event_id, safe='')}"
    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"source_id must not exceed {MAX_SOURCE_ID_LENGTH} characters")
    return source_id


def _parse_archive(content: bytes) -> _PiArchive:
    try:
        decoded = content.decode("utf-8-sig")
    except UnicodeError as error:
        raise ValueError("Pi session must be valid UTF-8") from error

    rows: list[dict[str, JsonValue]] = []
    for line in decoded.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError("Pi session must contain valid JSONL") from error
        if not isinstance(value, dict):
            raise ValueError("every Pi session line must be a JSON object")
        rows.append(cast(dict[str, JsonValue], value))

    if not rows:
        raise ValueError("Pi session is empty")
    header = rows[0]
    session_id = header.get("id")
    version = header.get("version")
    if (
        header.get("type") != "session"
        or not isinstance(session_id, str)
        or not session_id
        or isinstance(version, bool)
        or not isinstance(version, int)
        or version != 3
    ):
        raise ValueError("Pi session header must declare type=session, version=3, and a non-empty id")

    records = tuple(rows[1:])
    by_id: dict[str, dict[str, JsonValue]] = {}
    for record in records:
        event_id = record.get("id")
        event_type = record.get("type")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("every Pi event must have a non-empty string id")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("every Pi event must have a non-empty string type")
        if event_id in by_id:
            raise ValueError(f"duplicate Pi event id: {event_id}")
        if "parentId" not in record:
            raise ValueError(f"Pi event {event_id} must declare parentId")
        parent_id = record.get("parentId")
        if parent_id is not None:
            if not isinstance(parent_id, str) or not parent_id:
                raise ValueError("parentId must be null or a non-empty string")
            if parent_id not in by_id:
                raise ValueError(f"Pi event {event_id} references a missing or later parent {parent_id}")
        by_id[event_id] = record
    return _PiArchive(session_id=session_id, records=records, by_id=by_id)


def _selected_events(archive: _PiArchive, leaf_id: str | None) -> tuple[PiEventInput, ...]:
    if not archive.records:
        if leaf_id is not None:
            raise ValueError(f"Pi session leaf does not exist: {leaf_id}")
        return ()
    selected_id = leaf_id if leaf_id is not None else cast(str, archive.records[-1]["id"])
    if selected_id not in archive.by_id:
        raise ValueError(f"Pi session leaf does not exist: {selected_id}")

    path: list[dict[str, JsonValue]] = []
    current_id: str | None = selected_id
    while current_id is not None:
        try:
            record = archive.by_id[current_id]
        except KeyError as error:
            raise ValueError(f"Pi session history references a missing parent: {current_id}") from error
        path.append(record)
        parent_id = record.get("parentId")
        current_id = cast(str | None, parent_id)
    path.reverse()

    events = tuple(
        event
        for record in path
        if (event := _event_from_record(archive.session_id, record)) is not None
    )
    for event in events:
        pi_event_source_id(event.session_id, event.event_id)
    return events


def _event_from_record(session_id: str, record: dict[str, JsonValue]) -> PiEventInput | None:
    event_type = cast(str, record["type"])
    event_id = cast(str, record["id"])
    parent_id = cast(str | None, record.get("parentId"))

    if event_type == "message":
        message = record.get("message")
        if not isinstance(message, dict):
            return None
        role = message.get("role")
        if not isinstance(role, str) or not role:
            return None
        if role == "bashExecution":
            command = message.get("command")
            output = message.get("output")
            command_text = command if isinstance(command, str) else ""
            output_text = output if isinstance(output, str) else ""
            body = f"$ {command_text}\n{output_text}" if command_text or output_text else ""
        elif role == "toolResult":
            tool_name = message.get("toolName")
            tool_call_id = message.get("toolCallId")
            body_text = _text_blocks(message.get("content"))
            if isinstance(tool_name, str) and isinstance(tool_call_id, str):
                body = f"[tool_result {tool_name} {tool_call_id}] {body_text}"
            else:
                body = body_text
        else:
            body = _message_body(message.get("content"))
    elif event_type == "custom_message":
        role = "custom"
        body = _text_blocks(record.get("content"))
    elif event_type in {"compaction", "branch_summary"}:
        role = event_type
        summary = record.get("summary")
        body = summary if isinstance(summary, str) else ""
    else:
        return None

    if not body:
        return None
    return PiEventInput(
        session_id=session_id,
        event_id=event_id,
        parent_id=parent_id,
        event_type=event_type,
        role=role,
        content=f"{role}: {body}",
        record=record,
    )


def _message_body(content: JsonValue | None) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            text = block.get("text")
            if isinstance(text, str) and text:
                parts.append(text)
        elif block_type == "toolCall":
            name = block.get("name")
            call_id = block.get("id")
            if isinstance(name, str) and isinstance(call_id, str):
                arguments = block.get("arguments", {})
                compact_arguments = json.dumps(
                    arguments,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                parts.append(f"[tool_call {name} {call_id}] {compact_arguments}")
    return "\n".join(parts)


def _text_blocks(content: JsonValue | None) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = [
        text
        for block in content
        if isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance((text := block.get("text")), str)
        and text
    ]
    return "\n".join(parts)


def _load_checkpoint(value: JsonValue, session_id: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or set(value) != {"schema", "session_id", "events"}:
        raise ValueError("invalid Pi session connector checkpoint")
    if value.get("schema") != _CHECKPOINT_SCHEMA or value.get("session_id") != session_id:
        raise ValueError("Pi session connector checkpoint does not match this session")
    events = value.get("events")
    if not isinstance(events, dict):
        raise ValueError("invalid Pi session connector checkpoint events")
    imported: dict[str, str] = {}
    for source_id, digest in events.items():
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("invalid Pi event identity in checkpoint")
        if not _is_record_digest(digest):
            raise ValueError("invalid Pi event digest in checkpoint")
        imported[source_id] = digest
    return imported


def _archive_record_digests(archive: _PiArchive) -> dict[str, str]:
    digests: dict[str, str] = {}
    for record in archive.records:
        event_id = cast(str, record["id"])
        try:
            source_id = pi_event_source_id(archive.session_id, event_id)
        except ValueError:
            continue
        digests[source_id] = _record_digest(record)
    return digests


def _record_digest(record: dict[str, JsonValue]) -> str:
    encoded = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _is_record_digest(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        return False
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError:
        return False
    return value == value.lower()


PI_EVENT_SOURCE_DEFINITION = AdapterSourceDefinition(
    PiEventSourceAdapter(),
    projections=(PiEventTextEvidenceProjection(),),
)

__all__ = [
    "PI_EVENT_SOURCE_DEFINITION",
    "PI_EVENT_SOURCE_NAME",
    "PI_SESSION_CONNECTOR_NAME",
    "PI_SESSION_CONNECTOR_VERSION",
    "PiEventInput",
    "PiEventSource",
    "PiEventSourceAdapter",
    "PiEventTextEvidenceProjection",
    "PiSessionConnector",
    "PiSessionInput",
    "parse_pi_session",
    "pi_event_source_id",
]
