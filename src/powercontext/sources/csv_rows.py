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

"""Captured CSV rows with stable, document-scoped primary-key identities."""

from __future__ import annotations

import csv
import io
import json
from typing import Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, JsonValue, field_validator, model_validator

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    AdapterSourceDefinition,
    Source,
    SourceMaterialization,
    TextEvidence,
)

CSV_ROW_SOURCE_NAME = "csv-row"


class CSVDocumentInput(BaseModel):
    """One complete, offline CSV document and the identity used to index its rows."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    primary_key: str
    content: bytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _validate_document_id(value)

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        return _validate_primary_key(value)


class CSVRowInput(BaseModel):
    """A row's exact field values and the identity of the document that contains it."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _validate_document_id(value)

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        return _validate_primary_key(value)

    @field_validator("primary_key_value")
    @classmethod
    def validate_primary_key_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("primary_key_value must not be blank")
        return value

    @model_validator(mode="after")
    def validate_row_shape(self) -> CSVRowInput:
        _validate_row_values(self.primary_key, self.columns, self.values)
        if self.values[self.columns.index(self.primary_key)] != self.primary_key_value:
            raise ValueError("primary_key_value must match the named column value")
        return self


class CSVRowSource(Source):
    """An immutable captured CSV row represented as independent evidence."""

    model_config = ConfigDict(extra="forbid")

    materialization: Literal[SourceMaterialization.CAPTURED] = SourceMaterialization.CAPTURED
    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str) -> str:
        if not value or value != value.strip() or len(value) > MAX_SOURCE_ID_LENGTH:
            raise ValueError("source_id must be a bounded, non-empty trimmed string")
        return value

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _validate_document_id(value)

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        return _validate_primary_key(value)

    @field_validator("primary_key_value")
    @classmethod
    def validate_primary_key_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("primary_key_value must not be blank")
        return value

    @model_validator(mode="after")
    def validate_source_row(self) -> CSVRowSource:
        if self.name != self.source_id:
            raise ValueError("name and source_id must be identical")
        if self.source_id != csv_row_source_id(self.document_id, self.primary_key_value):
            raise ValueError("source_id must be derived from document_id and primary_key_value")
        _validate_row_values(self.primary_key, self.columns, self.values)
        if self.values[self.columns.index(self.primary_key)] != self.primary_key_value:
            raise ValueError("primary_key_value must match the named column value")
        return self


class CSVRowTextEvidenceProjection:
    """Expose one complete CSV row through the standard text-evidence projection."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        row = dict(zip(source.columns, source.values, strict=True))
        metadata: dict[str, JsonValue] = {
            "document_id": source.document_id,
            "primary_key": source.primary_key,
            "primary_key_value": source.primary_key_value,
            "columns": list(source.columns),
            "row": row,
        }
        return TextEvidence(
            source_type=CSV_ROW_SOURCE_NAME,
            source_id=source.source_id,
            content=json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            metadata=metadata,
        )


class CSVRowSourceAdapter:
    """Resolve parsed rows into captured Sources and read them back losslessly."""

    input_class = CSVRowInput
    name = CSV_ROW_SOURCE_NAME
    source_class = CSVRowSource

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        source_id = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=source_id,
            source_id=source_id,
            document_id=value.document_id,
            primary_key=value.primary_key,
            primary_key_value=value.primary_key_value,
            columns=value.columns,
            values=value.values,
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )


def parse_csv_document(value: CSVDocumentInput, /) -> tuple[CSVRowInput, ...]:
    """Parse a strict UTF-8 CSV document into exact primary-key-addressed rows."""

    try:
        decoded = value.content.decode("utf-8-sig")
        records = csv.reader(io.StringIO(decoded, newline=""), strict=True)
        header = next(records, None)
        if header is None:
            raise ValueError("CSV document must contain a header")
        _validate_row_values(value.primary_key, tuple(header), tuple(header), require_primary_key=False)
        columns = tuple(header)
        primary_key_index = columns.index(value.primary_key)
        rows: list[CSVRowInput] = []
        seen_primary_keys: set[str] = set()
        for record in records:
            if not record:
                continue
            values = tuple(record)
            if len(values) != len(columns):
                raise ValueError("CSV row width must match the header")
            primary_key_value = values[primary_key_index]
            if not primary_key_value.strip():
                raise ValueError("CSV primary key values must not be blank")
            if primary_key_value in seen_primary_keys:
                raise ValueError("CSV primary key values must be unique within a document")
            csv_row_source_id(value.document_id, primary_key_value)
            seen_primary_keys.add(primary_key_value)
            rows.append(
                CSVRowInput(
                    document_id=value.document_id,
                    primary_key=value.primary_key,
                    primary_key_value=primary_key_value,
                    columns=columns,
                    values=values,
                )
            )
    except (csv.Error, UnicodeError) as error:
        raise ValueError("invalid CSV document") from error
    return tuple(rows)


def csv_row_source_id(document_id: str, primary_key_value: str, /) -> str:
    """Build the stable, delimiter-safe identity for one CSV row."""

    _validate_document_id(document_id)
    if not primary_key_value.strip():
        raise ValueError("primary_key_value must not be blank")
    source_id = (
        f"csv:{quote(document_id, safe='')}:"
        f"{quote(primary_key_value, safe='')}"
    )
    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"source_id must not exceed {MAX_SOURCE_ID_LENGTH} characters")
    return source_id


def _validate_document_id(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError("document_id must be non-empty and must not contain surrounding whitespace")
    return value


def _validate_primary_key(value: str) -> str:
    if not value:
        raise ValueError("primary_key must not be empty")
    return value


def _validate_row_values(
    primary_key: str,
    columns: tuple[str, ...],
    values: tuple[str, ...],
    *,
    require_primary_key: bool = True,
) -> None:
    if not columns or any(not column.strip() for column in columns):
        raise ValueError("CSV columns must be non-empty and non-blank")
    if len(set(columns)) != len(columns):
        raise ValueError("CSV columns must be unique")
    if len(values) != len(columns):
        raise ValueError("CSV values must match the column count")
    if require_primary_key and primary_key not in columns:
        raise ValueError("primary_key must exactly match one CSV column")


CSV_ROW_SOURCE_DEFINITION = AdapterSourceDefinition(
    CSVRowSourceAdapter(),
    projections=(CSVRowTextEvidenceProjection(),),
)

__all__ = [
    "CSV_ROW_SOURCE_DEFINITION",
    "CSV_ROW_SOURCE_NAME",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "CSVRowSourceAdapter",
    "CSVRowTextEvidenceProjection",
    "csv_row_source_id",
    "parse_csv_document",
]
