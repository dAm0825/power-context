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

"""Automatic Source-count admission for Memory and Topic Memory."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Literal

from sqlalchemy import select

from powercontext.builtin.artifacts.topic_memory import TOPIC_MEMORY_SOURCE_WINDOW_BINDING
from powercontext.builtin.persistence.cursors import SourceCursorRepository
from powercontext.builtin.persistence.database import AsyncDatabase
from powercontext.builtin.persistence.tables import SOURCE_JOURNAL_HEADS_TABLE
from powercontext.builtin.triggers import SOURCE_WINDOW_TRIGGER_NAME

if TYPE_CHECKING:
    from powercontext.builtin.runtime.application import BuiltinRuntime

logger = logging.getLogger(__name__)

_POLL_SECONDS = 0.5
_TOPIC_REQUESTS = "topic-requests"
CountFamily = Literal["memory", "topic-memory"]


class SourceCountTriggerManager:
    """Admit one bounded processing call when a family's unconsumed Source count reaches its gate."""

    def __init__(
        self,
        runtime: BuiltinRuntime,
        database: AsyncDatabase,
        *,
        memory_threshold: int | None,
        topic_memory_threshold: int | None,
    ) -> None:
        self._runtime = runtime
        self._database = database
        self._thresholds: dict[CountFamily, int] = {}
        if memory_threshold is not None:
            self._thresholds["memory"] = memory_threshold
        if topic_memory_threshold is not None:
            self._thresholds["topic-memory"] = topic_memory_threshold
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._active: dict[tuple[str, CountFamily], asyncio.Task[None]] = {}
        self._topic_requested: dict[str, tuple[int, float]] = {}
        self._cursors = SourceCursorRepository()

    @property
    def enabled(self) -> bool:
        return bool(self._thresholds)

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="source-count-triggers")
        self.notify()

    def notify(self, scope_id: str | None = None) -> None:
        if not self.enabled:
            return
        self._wake.set()

    async def close(self) -> None:
        self._stop.set()
        self._wake.set()
        active = tuple(self._active.values())
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            try:
                scopes = await self._runtime._scope_ids_for_count_triggers()
                for scope_id in scopes:
                    for family in self._thresholds:
                        key = (scope_id, family)
                        task = self._active.get(key)
                        if task is not None and not task.done():
                            continue
                        if task is not None:
                            self._active.pop(key, None)
                        task = asyncio.create_task(
                            self._check(scope_id, family),
                            name=f"source-count-{family}-{scope_id}",
                        )
                        self._active[key] = task
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("Source count trigger scan failed: %s", error)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=_POLL_SECONDS)

    async def _check(self, scope_id: str, family: CountFamily) -> None:
        try:
            head, cursor = await self._progress(scope_id, family)
            pending = max(0, head - cursor)
            threshold = self._thresholds[family]
            if pending < threshold:
                return
            if family == "memory":
                await self._runtime.memory.for_scope(scope_id).flush(limit=self._runtime.source_window_limit)
                return
            previous = self._topic_requested.get(scope_id)
            if previous is not None and previous[0] == head:
                return
            accepted = await self._runtime.topic_memory.for_scope(scope_id).flush()
            if accepted.status == "accepted":
                self._topic_requested[scope_id] = (head, asyncio.get_running_loop().time())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Source count trigger %s failed for scope %s: %s", family, scope_id, error)

    async def _progress(self, scope_id: str, family: CountFamily) -> tuple[int, int]:
        binding = SOURCE_WINDOW_TRIGGER_NAME if family == "memory" else TOPIC_MEMORY_SOURCE_WINDOW_BINDING
        async with self._database.transaction() as connection:
            head = await connection.scalar(
                select(SOURCE_JOURNAL_HEADS_TABLE.c.position).where(SOURCE_JOURNAL_HEADS_TABLE.c.scope_id == scope_id)
            )
            cursor = await self._cursors.load(connection, scope_id, binding)
        return (
            0 if head is None else int(head),
            0 if cursor is None else cursor.cursor.sequence,
        )
