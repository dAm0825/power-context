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

"""Page-complete PDF Sources and exact page citation retrieval."""

from __future__ import annotations

import hashlib
import io
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator
from pypdf import PdfReader

from powercontext.artifacts import MemoryCitation
from powercontext.builtin.runtime.models import GetMemoryEntryRequest, SearchMemoryRequest
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    AdapterSourceDefinition,
    Source,
    SourceMaterialization,
    TextEvidence,
)

PDF_PAGE_SOURCE_NAME = "pdf-page"
_QUERY_TERM = re.compile(r"[a-z]+|\d+")
_QUERY_STOP_WORDS = frozenset({
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "by",
    "did",
    "do",
    "does",
    "for",
    "from",
    "had",
    "has",
    "have",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
})


class PDFDocumentInput(BaseModel):
    """A complete PDF byte stream and the caller's document identity."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    content: bytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("blank-document-id")
        return value


class PDFPageInput(BaseModel):
    """The exact extractable text and physical position for one PDF page."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    document_sha256: str
    page_number: int = Field(ge=1)
    text: str

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("blank-document-id")
        return value

    @field_validator("document_sha256")
    @classmethod
    def validate_document_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("invalid-document-sha256")
        return value


class PDFPageSource(Source):
    """One captured physical page retaining its whole-document revision identity."""

    model_config = ConfigDict(extra="forbid")

    materialization: Literal[SourceMaterialization.CAPTURED] = SourceMaterialization.CAPTURED
    document_id: str
    document_sha256: str
    page_number: int = Field(ge=1)
    text: str

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("blank-document-id")
        return value

    @field_validator("document_sha256")
    @classmethod
    def validate_document_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("invalid-document-sha256")
        return value

    def model_post_init(self, __context: object) -> None:
        expected = pdf_page_source_id(self.document_id, self.document_sha256, self.page_number)
        if self.name != expected:
            raise ValueError("invalid-source-name")


class PDFPageTextEvidenceProjection:
    """Expose one exact PDF page through the standard text-evidence projection."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PDFPageSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PDFPageSource, /) -> TextEvidence:
        metadata: dict[str, JsonValue] = {
            "document_id": source.document_id,
            "document_sha256": source.document_sha256,
            "page_number": source.page_number,
        }
        return TextEvidence(
            source_type=PDF_PAGE_SOURCE_NAME,
            source_id=source.name,
            content=f"{source.document_id}\n{source.text}",
            metadata=metadata,
        )


class PDFPageSourceAdapter:
    """Resolve parsed pages into captured Sources and read them back losslessly."""

    input_class = PDFPageInput
    name = PDF_PAGE_SOURCE_NAME
    source_class = PDFPageSource

    async def resolve(self, value: PDFPageInput, /) -> PDFPageSource:
        return PDFPageSource(
            name=pdf_page_source_id(value.document_id, value.document_sha256, value.page_number),
            document_id=value.document_id,
            document_sha256=value.document_sha256,
            page_number=value.page_number,
            text=value.text,
        )

    async def read(self, source: PDFPageSource, /) -> PDFPageInput:
        return PDFPageInput(
            document_id=source.document_id,
            document_sha256=source.document_sha256,
            page_number=source.page_number,
            text=source.text,
        )


def parse_pdf_document(value: PDFDocumentInput, /) -> tuple[PDFPageInput, ...]:
    """Parse every physical page from one complete PDF, including blank pages."""

    if not value.content:
        raise ValueError("empty-pdf")
    document_sha256 = hashlib.sha256(value.content).hexdigest()
    try:
        reader = PdfReader(io.BytesIO(value.content), strict=False)
        if reader.is_encrypted and not reader.decrypt(""):
            raise ValueError("password-required")  # noqa: TRY301
        pages = tuple(
            PDFPageInput(
                document_id=value.document_id,
                document_sha256=document_sha256,
                page_number=index,
                text=page.extract_text() or "",
            )
            for index, page in enumerate(reader.pages, start=1)
        )
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("invalid-pdf") from error
    if not pages:
        raise ValueError("empty-pdf")
    return pages


async def search_pdf_pages(
    memory,
    question: str,
    limit: int = 10,
) -> tuple[MemoryCitation, ...]:
    """Search page evidence and return citation identities for distinct pages."""

    if not question.strip():
        raise ValueError("blank-question")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("invalid-limit")
    page = await memory.search(
        SearchMemoryRequest(
            query=question,
            mode="fts",
            limit=max(limit * 4, 32),
        )
    )
    query_terms = _query_terms(question)
    ranked = sorted(
        page.hits,
        key=lambda hit: (
            -_page_relevance(query_terms, hit.text),
            -hit.score,
            hit.entry_id,
        ),
    )

    citations: list[MemoryCitation] = []
    seen_pages: set[tuple[str, str]] = set()
    for hit in ranked:
        citation = MemoryCitation(
            memory_ref=hit.memory_ref,
            entry_id=hit.entry_id,
            entry_version_id=hit.entry_version_id,
        )
        entry = await memory.get(GetMemoryEntryRequest(citation=citation))
        page_sources = tuple(
            reference for reference in entry.entry.sources if reference.source_type == PDF_PAGE_SOURCE_NAME
        )
        if len(page_sources) != 1:
            continue
        identity = (page_sources[0].source_type, page_sources[0].source_id)
        if identity in seen_pages:
            continue
        seen_pages.add(identity)
        citations.append(citation)
        if len(citations) >= limit:
            break
    return tuple(citations)


def pdf_page_source_id(document_id: str, document_sha256: str, page_number: int, /) -> str:
    """Build a stable identity for one page of one exact document revision."""

    identity = f"{document_id}\0{document_sha256}\0{page_number}".encode()
    return f"pdf:{hashlib.sha256(identity).hexdigest()}"


def _query_terms(value: str) -> tuple[str, ...]:
    terms = tuple(term for term in _QUERY_TERM.findall(value.casefold()) if term not in _QUERY_STOP_WORDS)
    return tuple(dict.fromkeys(terms))


def _page_relevance(query_terms: tuple[str, ...], text: str) -> float:
    if not query_terms:
        return 0.0
    page_terms = set(_QUERY_TERM.findall(text.casefold()))
    matched = sum(term in page_terms for term in query_terms)
    numeric = sum(term.isdigit() and term in text for term in query_terms)
    return matched / len(query_terms) + numeric * 0.25


PDF_PAGE_SOURCE_DEFINITION = AdapterSourceDefinition(
    PDFPageSourceAdapter(),
    projections=(PDFPageTextEvidenceProjection(),),
)


__all__ = [
    "PDF_PAGE_SOURCE_DEFINITION",
    "PDF_PAGE_SOURCE_NAME",
    "PDFDocumentInput",
    "PDFPageInput",
    "PDFPageSource",
    "PDFPageSourceAdapter",
    "PDFPageTextEvidenceProjection",
    "parse_pdf_document",
    "pdf_page_source_id",
    "search_pdf_pages",
]
