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

"""HTTP cross-encoder adapter for globally consistent Memory reranking."""

from __future__ import annotations

import json
import math
from typing import Any

import httpx

from powercontext.builtin.artifacts.memory.models import MemoryHit
from powercontext.builtin.artifacts.memory.reranking import MemoryRerankDecision, _validate_bounds
from powercontext.builtin.inference import InferenceUsage


class HTTPMemoryReranker:
    """Score bounded candidate batches and select from the complete scored pool."""

    def __init__(
        self,
        endpoint: str,
        *,
        client: httpx.AsyncClient,
        batch_size: int = 32,
        policy_id: str = "http-cross-encoder-v1",
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint:
            raise ValueError("endpoint must be a non-empty string")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if not isinstance(policy_id, str) or not policy_id:
            raise ValueError("policy_id must be a non-empty string")
        self.endpoint = endpoint
        self.policy_id = policy_id
        self._client = client
        self._batch_size = batch_size

    async def rerank(
        self,
        query: str,
        candidates: tuple[MemoryHit, ...],
        limit: int,
        /,
    ) -> MemoryRerankDecision:
        """Return the globally highest-scoring original candidate ranks."""

        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("rerank limit must be an integer")
        _validate_bounds(len(candidates), limit)

        scores: dict[int, float] = {}
        request_count = 0
        for start in range(0, len(candidates), self._batch_size):
            batch = tuple(
                (rank, candidates[rank - 1])
                for rank in range(start + 1, min(start + self._batch_size, len(candidates)) + 1)
            )
            response = await self._client.post(
                self.endpoint,
                json={
                    "query": query,
                    "candidates": [
                        {"rank": rank, "text": candidate.text}
                        for rank, candidate in batch
                    ],
                },
            )
            request_count += 1
            response.raise_for_status()
            scores.update(_parse_scores(response, expected_ranks={rank for rank, _ in batch}))

        selected = tuple(
            rank
            for rank, _score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:limit]
        )
        return MemoryRerankDecision(
            selected_ranks=selected,
            usage=InferenceUsage(requests=request_count),
        )


def _parse_scores(response: httpx.Response, *, expected_ranks: set[int]) -> dict[int, float]:
    try:
        payload: Any = json.loads(response.content)
    except (TypeError, ValueError) as error:
        raise ValueError("reranker response must be valid JSON") from error
    if not isinstance(payload, dict) or set(payload) != {"scores"}:
        raise ValueError("reranker response must contain only scores")
    raw_scores = payload["scores"]
    if not isinstance(raw_scores, list):
        raise ValueError("reranker scores must be an array")

    scores: dict[int, float] = {}
    for item in raw_scores:
        if not isinstance(item, dict) or set(item) != {"rank", "score"}:
            raise ValueError("each reranker score must contain only rank and score")
        rank = item["rank"]
        score = item["score"]
        if isinstance(rank, bool) or not isinstance(rank, int) or rank not in expected_ranks:
            raise ValueError("reranker returned an invalid rank")
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ValueError("reranker score must be numeric")
        numeric_score = float(score)
        if not math.isfinite(numeric_score):
            raise ValueError("reranker score must be finite")
        if rank in scores:
            raise ValueError("reranker returned a duplicate rank")
        scores[rank] = numeric_score
    if set(scores) != expected_ranks:
        raise ValueError("reranker response must cover every candidate in the batch")
    return scores


__all__ = ["HTTPMemoryReranker"]
