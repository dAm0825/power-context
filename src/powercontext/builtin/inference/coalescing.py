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

"""Coalesce concurrent Embedding work and retain bounded successful vectors."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from itertools import count

from powercontext.builtin.artifacts.memory import EmbeddingProfile
from powercontext.builtin.inference.models import EmbeddingResult, EmbeddingVector, InferenceUsage
from powercontext.builtin.inference.protocols import EmbeddingModel, QueryEmbeddingModel


@dataclass(frozen=True, slots=True)
class _SharedVector:
    vector: EmbeddingVector
    batch_id: int
    usage: InferenceUsage


class CoalescingEmbeddingModel:
    """Share in-flight text embeddings across callers and cache successful vectors."""

    def __init__(
        self,
        model: EmbeddingModel,
        *,
        max_batch_size: int = 64,
        coalesce_ms: float = 5.0,
        cache_size: int = 1024,
    ) -> None:
        if isinstance(max_batch_size, bool) or not isinstance(max_batch_size, int) or max_batch_size < 1:
            raise ValueError("invalid-max-batch-size")
        if isinstance(coalesce_ms, bool) or not isinstance(coalesce_ms, int | float) or coalesce_ms < 0:
            raise ValueError("invalid-coalesce-ms")
        if isinstance(cache_size, bool) or not isinstance(cache_size, int) or cache_size < 0:
            raise ValueError("invalid-cache-size")
        self._model = model
        self._max_batch_size = max_batch_size
        self._coalesce_seconds = float(coalesce_ms) / 1000
        self._cache_size = cache_size
        self._cache: OrderedDict[str, EmbeddingVector] = OrderedDict()
        self._inflight: dict[str, asyncio.Future[_SharedVector]] = {}
        self._pending: list[tuple[str, asyncio.Future[_SharedVector]]] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._batch_ids = count(1)

    @property
    def profile(self) -> EmbeddingProfile:
        return self._model.profile

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        if not texts:
            return EmbeddingResult(vectors=(), usage=InferenceUsage(requests=0))

        unique = tuple(dict.fromkeys(texts))
        shared: dict[str, asyncio.Future[_SharedVector]] = {}
        for text in unique:
            cached = self._cache_get(text)
            if cached is not None:
                future: asyncio.Future[_SharedVector] = asyncio.get_running_loop().create_future()
                future.set_result(_SharedVector(cached, 0, InferenceUsage(requests=0)))
            else:
                future = self._inflight.get(text)
                if future is None:
                    future = asyncio.get_running_loop().create_future()
                    self._inflight[text] = future
                    self._pending.append((text, future))
                self._ensure_flush()
            shared[text] = future

        resolved = await asyncio.gather(*(asyncio.shield(shared[text]) for text in unique))
        by_text = dict(zip(unique, resolved, strict=True))

        usages: dict[int, InferenceUsage] = {}
        for value in resolved:
            if value.batch_id:
                usages.setdefault(value.batch_id, value.usage)
        requests = sum(usage.requests for usage in usages.values())
        input_tokens = _sum_optional(usage.input_tokens for usage in usages.values())
        output_tokens = _sum_optional(usage.output_tokens for usage in usages.values())
        return EmbeddingResult(
            vectors=tuple(by_text[text].vector for text in texts),
            usage=InferenceUsage(
                requests=requests,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            ),
        )

    async def embed_query(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Use the base model's query-specific embedding capability when it has one."""

        base = self._model
        if isinstance(base, QueryEmbeddingModel):
            return await base.embed_query(texts)
        return await self.embed(texts)

    def _cache_get(self, text: str) -> EmbeddingVector | None:
        if self._cache_size == 0:
            return None
        vector = self._cache.get(text)
        if vector is not None:
            self._cache.move_to_end(text)
        return vector

    def _cache_put(self, text: str, vector: EmbeddingVector) -> None:
        if self._cache_size == 0:
            return
        self._cache[text] = vector
        self._cache.move_to_end(text)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    def _ensure_flush(self) -> None:
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_pending())

    async def _flush_pending(self) -> None:
        try:
            if self._coalesce_seconds:
                await asyncio.sleep(self._coalesce_seconds)
            while self._pending:
                batch = self._pending[: self._max_batch_size]
                del self._pending[: self._max_batch_size]
                await self._embed_batch(batch)
                if self._pending and self._coalesce_seconds:
                    await asyncio.sleep(self._coalesce_seconds)
        finally:
            self._flush_task = None
            if self._pending:
                self._ensure_flush()

    async def _embed_batch(self, batch: list[tuple[str, asyncio.Future[_SharedVector]]]) -> None:
        texts = tuple(text for text, _ in batch)
        try:
            result = await self._model.embed(texts)
            if len(result.vectors) != len(texts):
                raise ValueError("invalid-vector-count")  # noqa: TRY301
        except BaseException as error:
            for text, future in batch:
                if self._inflight.get(text) is future:
                    del self._inflight[text]
                if not future.done():
                    future.set_exception(error)
                    # Mark the exception observed when every waiter has already been cancelled.
                    future.exception()
            return

        batch_id = next(self._batch_ids)
        for (text, future), vector in zip(batch, result.vectors, strict=True):
            self._cache_put(text, vector)
            if self._inflight.get(text) is future:
                del self._inflight[text]
            if not future.done():
                future.set_result(_SharedVector(vector, batch_id, result.usage))


def _sum_optional(values) -> int | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None
