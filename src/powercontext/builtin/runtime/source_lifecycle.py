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

"""Propagate Source availability through a scoped evidence dependency graph."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, StrictBool, StrictInt, field_validator
from sqlalchemy import Column, MetaData, String, Table, Text, select

from powercontext.artifacts import ArtifactRef, MemoryCitation
from powercontext.builtin.persistence.database import AsyncDatabase
from powercontext.sources import SourceRef

if TYPE_CHECKING:
    from powercontext.builtin.artifacts.topic_memory import TopicMemorySearchHit, TopicMemorySearchResult
    from powercontext.builtin.runtime.application import BuiltinRuntime

LifecycleFamily = Literal["source", "memory", "topic"]
LifecyclePolicy = Literal["all", "any"]


class SourceLifecycleError(ValueError):
    """Report an invalid graph, event, reference, or lifecycle transition."""


class SourceLifecycleEvent(BaseModel):
    """One validated availability event for a registered Source."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    node_id: str
    revision: StrictInt
    sequence: StrictInt
    available: StrictBool
    reason: str

    @field_validator("node_id", "reason")
    @classmethod
    def validate_non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("blank")
        return value

    @field_validator("revision", "sequence")
    @classmethod
    def validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("non-positive")
        return value


class SourceLifecycleApplication:
    """Select the lifecycle service for one Scope."""

    def __init__(self, runtime: BuiltinRuntime) -> None:
        self._runtime = runtime

    def for_scope(self, scope_id: str, /) -> ScopedSourceLifecycle:
        return ScopedSourceLifecycle(self._runtime, scope_id)


class ScopedSourceLifecycle:
    """Persist one scoped dependency DAG and mirror its state into native artifacts."""

    def __init__(self, runtime: BuiltinRuntime, scope_id: str) -> None:
        self._runtime = runtime
        self.scope_id = scope_id
        self._database = _runtime_database(runtime)
        self._metadata = MetaData()
        self._nodes = Table(
            "pc_source_lifecycle_nodes",
            self._metadata,
            Column("scope_id", String(256), primary_key=True),
            Column("node_id", String(512), primary_key=True),
            Column("payload", Text, nullable=False),
            extend_existing=True,
        )
        self._schema_ready = False
        self._fallback: dict[str, dict[str, Any]] = {}

    async def register_source(
        self,
        node_id: str,
        source_ref: SourceRef,
        *,
        revision: int,
    ) -> None:
        _validate_node_id(node_id)
        _validate_positive_integer(revision)
        async with self._runtime._context(self.scope_id) as context:
            sources = await context.sources.list()
            if not any(context.sources.catalog.as_ref(source) == source_ref for source in sources):
                raise SourceLifecycleError("unknown-source")
            async with self._runtime._locked(self.scope_id):
                payload = {
                    "node_id": node_id,
                    "family": "source",
                    "reference": source_ref.model_dump(mode="json"),
                    "parents": [],
                    "policy": "all",
                    "revision": revision,
                    "sequence": 0,
                    "available": True,
                    "reason": None,
                    "event_signature": None,
                }
                await self._register(node_id, payload)

    async def register_memory(
        self,
        node_id: str,
        citation: MemoryCitation,
        *,
        parents: Sequence[str],
        policy: LifecyclePolicy,
    ) -> None:
        if not isinstance(citation, MemoryCitation):
            raise SourceLifecycleError("invalid-memory")
        if not isinstance(parents, tuple):
            parents = tuple(parents)
        await self._runtime.memory.for_scope(self.scope_id).get(_get_memory_request(citation))
        payload = {
            "node_id": node_id,
            "family": "memory",
            "reference": citation.model_dump(mode="json"),
            "parents": list(parents),
            "policy": _validate_derived(node_id, parents, policy),
            "revision": None,
            "sequence": 0,
            "available": True,
            "reason": None,
            "event_signature": None,
        }
        async with self._runtime._context(self.scope_id), self._runtime._locked(self.scope_id):
            await self._register(node_id, payload)
            nodes = await self._load()
            await self._apply_memory_states(nodes, _active_nodes(nodes), reason="register derived Memory")

    async def register_topic(
        self,
        node_id: str,
        artifact_ref: ArtifactRef,
        *,
        parents: Sequence[str],
        policy: LifecyclePolicy,
    ) -> None:
        if not isinstance(artifact_ref, ArtifactRef) or artifact_ref.family != "topic-memory":
            raise SourceLifecycleError("invalid-topic")
        if not isinstance(parents, tuple):
            parents = tuple(parents)
        await self._runtime.records.for_scope(self.scope_id).get_artifact_revision(
            artifact_ref.family,
            artifact_ref.artifact_id,
            artifact_ref.revision,
        )
        payload = {
            "node_id": node_id,
            "family": "topic",
            "reference": artifact_ref.model_dump(mode="json"),
            "parents": list(parents),
            "policy": _validate_derived(node_id, parents, policy),
            "revision": None,
            "sequence": 0,
            "available": True,
            "reason": None,
            "event_signature": None,
        }
        async with self._runtime._context(self.scope_id), self._runtime._locked(self.scope_id):
            await self._register(node_id, payload)

    async def apply(self, event: SourceLifecycleEvent, /) -> list[str]:
        if not isinstance(event, SourceLifecycleEvent):
            raise SourceLifecycleError("invalid-event")
        async with self._runtime._context(self.scope_id), self._runtime._locked(self.scope_id):
            nodes = await self._load()
            source = nodes.get(event.node_id)
            if source is None or source["family"] != "source":
                raise SourceLifecycleError("unknown-source-node")
            if event.revision != source["revision"]:
                raise SourceLifecycleError("revision-mismatch")
            sequence = int(source["sequence"])
            signature = _event_signature(event)
            if event.sequence < sequence:
                raise SourceLifecycleError("stale-sequence")
            if event.sequence == sequence:
                if signature != source["event_signature"]:
                    raise SourceLifecycleError("sequence-conflict")
                await self._apply_memory_states(nodes, _active_nodes(nodes), reason=event.reason)
                return sorted(_active_nodes(nodes))

            prospective = {key: dict(value) for key, value in nodes.items()}
            prospective[event.node_id].update({
                "sequence": event.sequence,
                "available": event.available,
                "reason": event.reason,
                "event_signature": signature,
            })
            active = _active_nodes(prospective)
            await self._apply_memory_states(prospective, active, reason=event.reason)
            await self._save_node(prospective[event.node_id])
            return sorted(active)

    async def resolve(self, node_id: str, *, purpose: str):
        nodes = await self._load()
        node = nodes.get(node_id)
        if node is None:
            raise SourceLifecycleError("unknown-node")
        if purpose not in {"generation", "audit"}:
            raise SourceLifecycleError("unknown-purpose")
        active = _active_nodes(nodes)
        if purpose == "generation" and node_id not in active:
            raise SourceLifecycleError("unavailable")
        return _reference_from_payload(node["reference"], node["family"])

    async def filter_topic_hits(
        self,
        hits: Sequence[TopicMemorySearchHit],
    ) -> tuple[TopicMemorySearchHit, ...]:
        nodes = await self._load()
        topic_nodes = {
            _artifact_key(ArtifactRef.model_validate(value["reference"])): key
            for key, value in nodes.items()
            if value["family"] == "topic"
        }
        if not topic_nodes:
            return tuple(hits)
        active = _active_nodes(nodes)
        return tuple(hit for hit in hits if topic_nodes.get(_artifact_key(hit.artifact_ref), "") in active)

    async def filter_topic_result(self, result: TopicMemorySearchResult) -> TopicMemorySearchResult:
        hits = await self.filter_topic_hits(result.hits)
        if hits == result.hits:
            return result
        return result.model_copy(update={"hits": hits})

    async def _register(self, node_id: str, payload: dict[str, Any]) -> None:
        nodes = await self._load()
        existing = nodes.get(node_id)
        if existing is not None:
            if _binding_signature(existing) != _binding_signature(payload):
                raise SourceLifecycleError("binding-conflict")
            return
        parents = payload["parents"]
        if payload["family"] != "source":
            for parent in parents:
                if parent == node_id or parent not in nodes:
                    raise SourceLifecycleError("invalid-parent")
        await self._save_node(payload)

    async def _apply_memory_states(
        self,
        nodes: dict[str, dict[str, Any]],
        active: set[str],
        *,
        reason: str,
    ) -> None:
        memory_nodes = {
            key: MemoryCitation.model_validate(value["reference"])
            for key, value in nodes.items()
            if value["family"] == "memory"
        }
        if not memory_nodes:
            return
        async with self._runtime._context(self.scope_id) as context:
            service = context.artifacts.memory
            try:
                head = await service.head(context.artifacts.memory_artifact_id)
            except Exception as error:
                if error.__class__.__name__ != "ArtifactNotFoundError":
                    raise
                return
            manifest = {
                (entry.entry_id, entry.entry_version_id): entry.state for entry in head.content.manifest.entries
            }
            to_deactivate = []
            to_reactivate = []
            for node_id, citation in memory_nodes.items():
                entry = await service.validate_citation(citation)
                current = manifest.get((citation.entry_id, citation.entry_version_id))
                if node_id not in active and current == "active":
                    to_deactivate.append(entry)
                elif node_id in active and current == "inactive":
                    to_reactivate.append(entry)
            if to_deactivate:
                head = await service.forget(head, entries=tuple(to_deactivate), reason=reason)
            if to_reactivate:
                await service.reactivate(head, entries=tuple(to_reactivate), reason=reason)

    async def _ensure_schema(self) -> None:
        if self._schema_ready or self._database is None:
            return
        async with self._database.transaction() as connection:
            await connection.run_sync(self._metadata.create_all)
        self._schema_ready = True

    async def _load(self) -> dict[str, dict[str, Any]]:
        if self._database is None:
            return {key: dict(value) for key, value in self._fallback.items()}
        await self._ensure_schema()
        async with self._database.transaction() as connection:
            result = await connection.execute(
                select(self._nodes.c.node_id, self._nodes.c.payload).where(self._nodes.c.scope_id == self.scope_id)
            )
            return {node_id: json.loads(payload) for node_id, payload in result.all()}

    async def _save_node(self, payload: dict[str, Any]) -> None:
        node_id = payload["node_id"]
        if self._database is None:
            self._fallback[node_id] = dict(payload)
            return
        await self._ensure_schema()
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        async with self._database.transaction() as connection:
            result = await connection.execute(
                select(self._nodes.c.node_id).where(
                    self._nodes.c.scope_id == self.scope_id,
                    self._nodes.c.node_id == node_id,
                )
            )
            if result.scalar_one_or_none() is None:
                await connection.execute(
                    self._nodes.insert().values(
                        scope_id=self.scope_id,
                        node_id=node_id,
                        payload=encoded,
                    )
                )
            else:
                await connection.execute(
                    self._nodes
                    .update()
                    .where(
                        self._nodes.c.scope_id == self.scope_id,
                        self._nodes.c.node_id == node_id,
                    )
                    .values(payload=encoded)
                )


def _active_nodes(nodes: dict[str, dict[str, Any]]) -> set[str]:
    active = {key for key, value in nodes.items() if value["family"] == "source" and value["available"]}
    pending = [(key, value) for key, value in nodes.items() if value["family"] != "source"]
    while pending:
        next_pending = []
        changed = False
        for key, value in pending:
            support = [parent in active for parent in value["parents"]]
            available = all(support) if value["policy"] == "all" else any(support)
            if available:
                active.add(key)
                changed = True
            else:
                next_pending.append((key, value))
        if not changed:
            break
        pending = next_pending
    return active


def _validate_derived(node_id: str, parents: tuple[str, ...], policy: str) -> str:
    _validate_node_id(node_id)
    if policy not in {"all", "any"}:
        raise SourceLifecycleError("invalid-policy")
    if not parents:
        raise SourceLifecycleError("missing-parent")
    if len(set(parents)) != len(parents) or any(not parent.strip() for parent in parents):
        raise SourceLifecycleError("invalid-parents")
    return policy


def _validate_node_id(node_id: str) -> None:
    if not isinstance(node_id, str) or not node_id.strip():
        raise SourceLifecycleError("invalid-node-id")


def _validate_positive_integer(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SourceLifecycleError("invalid-positive-integer")


def _binding_signature(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["family"],
        json.dumps(payload["reference"], ensure_ascii=False, sort_keys=True),
        tuple(payload["parents"]),
        payload["policy"],
        payload.get("revision"),
    )


def _event_signature(event: SourceLifecycleEvent) -> dict[str, Any]:
    return {
        "revision": event.revision,
        "sequence": event.sequence,
        "available": event.available,
        "reason": event.reason,
    }


def _reference_from_payload(payload: dict[str, Any], family: LifecycleFamily):
    if family == "source":
        return SourceRef.model_validate(payload)
    if family == "memory":
        return MemoryCitation.model_validate(payload)
    return ArtifactRef.model_validate(payload)


def _artifact_key(reference: ArtifactRef) -> tuple[str, str, int]:
    return reference.family, reference.artifact_id, reference.revision


def _runtime_database(runtime: BuiltinRuntime) -> AsyncDatabase | None:
    database = getattr(getattr(runtime, "_provider", None), "database", None)
    return database if isinstance(database, AsyncDatabase) else None


def _get_memory_request(citation: MemoryCitation):
    from powercontext.builtin.runtime.models import GetMemoryEntryRequest

    return GetMemoryEntryRequest(citation=citation)
