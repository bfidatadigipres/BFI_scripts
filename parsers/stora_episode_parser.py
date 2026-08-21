"""
Pydantic models for the PA Media JSON payload.

Notes
- Unknown/extra keys are forbidden (except where explicitly allowed)
- Date-time fields are parsed as `datetime` when present.
- Strict input validation for security hardening.

2026
"""

from __future__ import annotations

import json
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

MAX_JSON_SIZE = 5 * 1024 * 1024  # 5 MB
HTTPS_URL_RE = re.compile(r"^https://[^\s]+$")

class UnexpectedFieldError(ValueError):
    """Raised if a JSON contains unanticipated fields."""


class PayloadTooLargeError(ValueError):
    """Raised if the raw JSON payload exceeds the maximum allowed size."""


def _validate_https_url(value: Optional[str]) -> Optional[str]:
    if value is not None and not HTTPS_URL_RE.match(value):
        raise ValueError(f"URL must start with https://: {value}")
    return value


def _try_int(value: Any) -> Optional[int]:
    """Coerce to int; return None if not coercible (let Pydantic handle it)."""
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    if isinstance(value, float):
        return int(value)
    return None


def _validate_non_negative(value: Any) -> Any:
    if value is None:
        return value
    n = _try_int(value)
    if n is not None and n < 0:
        raise ValueError(f"Value must be non-negative: {value}")
    return value


def _extract_extra_field_errors(e: ValidationError) -> List[Tuple[str, str]]:
    """
    Returns list of (json_path, message) for extra-field errors.
    Pydantic v2 uses error type: 'extra_forbidden' when extra='forbid'.
    """
    out: List[Tuple[str, str]] = []
    for err in e.errors():
        if err.get("type") == "extra_forbidden":
            loc = err.get("loc", ())
            path = "$"
            for part in loc:
                if isinstance(part, int):
                    path += f"[{part}]"
                else:
                    path += f".{part}"
            out.append((path, err.get("msg", "Extra field not permitted")))
    return out


class APIModel(BaseModel):
    """Base model: forbids unexpected fields."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Summary(APIModel):
    short: Optional[str] = Field(default=None, max_length=500)
    medium: Optional[str] = Field(default=None, max_length=2000)
    long: Optional[str] = Field(default=None, max_length=5000)
    welsh: Optional[str] = Field(default=None, max_length=5000)
    supplemental: Optional[str] = Field(default=None, max_length=5000)


class Category(APIModel):
    code: Optional[str] = Field(default=None, max_length=100)
    name: Optional[str] = Field(default=None, max_length=100)
    dvb: Optional[str] = Field(default=None, max_length=10)


class Subject(APIModel):
    code: Optional[str] = Field(default=None, max_length=100)
    profile: Optional[str] = Field(default=None, max_length=50)


class RenditionDefault(APIModel):
    width: Optional[int] = None
    height: Optional[int] = None
    href: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("width", "height", mode="before")
    @classmethod
    def _non_negative_resolution(cls, v: Any) -> Any:
        return _validate_non_negative(v)

    @field_validator("href", mode="before")
    @classmethod
    def _validate_href(cls, v: Optional[str]) -> Optional[str]:
        return _validate_https_url(v)


class Rendition(APIModel):
    default: Optional[RenditionDefault] = None


class Media(APIModel):
    kind: Optional[str] = Field(default=None, max_length=50)
    rendition: Optional[Rendition] = None
    copyright: Optional[str] = Field(default=None, max_length=500)
    expiry: Optional[datetime] = None


class Related(APIModel):
    id: Optional[str] = Field(default=None, max_length=64)
    title: Optional[str] = Field(default=None, max_length=500)
    type: Optional[str] = Field(default=None, max_length=50)
    number: Optional[int] = None
    subject: Optional[List[Subject]] = None
    media: Optional[List[Media]] = None

    @field_validator("number", mode="before")
    @classmethod
    def _non_negative_number(cls, v: Any) -> Any:
        return _validate_non_negative(v)


class Link(APIModel):
    rel: Optional[str] = Field(default=None, max_length=50)
    href: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("href", mode="before")
    @classmethod
    def _validate_href(cls, v: Optional[str]) -> Optional[str]:
        return _validate_https_url(v)


class Character(APIModel):
    name: Optional[str] = Field(default=None, max_length=200)
    type: Optional[str] = Field(default=None, max_length=50)


class Deeplink(APIModel):
    """Broad model — allows extra keys since deeplink structure may vary."""

    model_config = ConfigDict(extra="allow")

    url: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("url", mode="before")
    @classmethod
    def _validate_url(cls, v: Optional[str]) -> Optional[str]:
        return _validate_https_url(v)


class VodProvider(APIModel):
    start: Optional[datetime] = None
    region: Optional[str] = Field(default=None, max_length=10)
    href: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("href", mode="before")
    @classmethod
    def _validate_href(cls, v: Optional[str]) -> Optional[str]:
        return _validate_https_url(v)


class Contributor(APIModel):
    id: Optional[str] = Field(default=None, max_length=64)
    name: Optional[str] = Field(default=None, max_length=200)
    dob: Optional[date] = None
    dod: Optional[date] = None
    from_: Optional[str] = Field(default=None, alias="from", max_length=200)
    gender: Optional[str] = Field(default=None, max_length=20)
    meta: Optional[Dict[str, str]] = None
    media: Optional[List[Media]] = None
    subject: Optional[List[Subject]] = None
    character: Optional[List[Character]] = None
    role: Optional[List[str]] = Field(default=None, max_length=50)

    @field_validator("dob", "dod", mode="before")
    @classmethod
    def _validate_date_fields(cls, v: Any) -> Any:
        if v is None:
            return v
        year = None
        if isinstance(v, date) and not isinstance(v, datetime):
            year = v.year
        elif isinstance(v, datetime):
            year = v.year
        elif isinstance(v, str):
            try:
                year = date.fromisoformat(v).year
            except ValueError:
                pass
        if year is not None and year < 1800:
            raise ValueError("Date year must be >= 1800")
        return v

    @model_validator(mode="before")
    @classmethod
    def _validate_meta_values(cls, values: Any) -> Any:
        if isinstance(values, dict):
            meta = values.get("meta")
            if isinstance(meta, dict):
                for k, v in meta.items():
                    if isinstance(v, str) and len(v) > 5000:
                        raise ValueError(f"meta value for '{k}' exceeds max length of 5000")
        return values


class Asset(APIModel):
    id: Optional[str] = Field(default=None, max_length=64)
    type: Optional[str] = Field(default=None, max_length=50)
    number: Optional[int] = None
    total: Optional[int] = None
    title: Optional[str] = Field(default=None, max_length=500)
    runtime: Optional[int] = None
    releaseDate: Optional[str] = Field(default=None, max_length=50)
    productionYear: Optional[int] = None

    attribute: Optional[List[str]] = Field(default=None, max_length=50)
    category: Optional[List[Category]] = None

    keywords: Optional[List[Any]] = None
    mood: Optional[List[Any]] = None
    themes: Optional[List[Any]] = None

    contributor: Optional[List[Contributor]] = None
    soundtrack: Optional[List[Any]] = None
    locations: Optional[List[Any]] = None

    certification: Optional[Dict[str, str]] = None
    meta: Optional[Dict[str, str]] = None
    summary: Optional[Summary] = None

    media: Optional[List[Media]] = None
    related: Optional[List[Related]] = None
    subject: Optional[List[Subject]] = None

    link: Optional[List[Link]] = None
    deeplink: Optional[List[Deeplink]] = None
    vod: Optional[Dict[str, VodProvider]] = None

    @field_validator("number", "total", "runtime", "productionYear", mode="before")
    @classmethod
    def _non_negative_int(cls, v: Any) -> Any:
        return _validate_non_negative(v)

    @field_validator("productionYear", mode="before")
    @classmethod
    def _validate_production_year(cls, v: Any) -> Any:
        if v is None:
            return v
        n = _try_int(v)
        if n is not None and (n < 1800 or n > 2100):
            raise ValueError("productionYear must be between 1800 and 2100")
        return v

    @model_validator(mode="before")
    @classmethod
    def _validate_meta_values(cls, values: Any) -> Any:
        if isinstance(values, dict):
            meta = values.get("meta")
            if isinstance(meta, dict):
                for k, v in meta.items():
                    if isinstance(v, str) and len(v) > 5000:
                        raise ValueError(f"meta value for '{k}' exceeds max length of 5000")
        return values


class Item(APIModel):
    id: Optional[str] = Field(default=None, max_length=64)
    title: Optional[str] = Field(default=None, max_length=500)

    date_time: Optional[datetime] = Field(default=None, alias="dateTime")
    duration: Optional[int] = None

    attribute: Optional[List[str]] = Field(default=None, max_length=50)
    certification: Optional[Dict[str, str]] = None
    summary: Optional[Summary] = None
    meta: Optional[Dict[str, str]] = None

    asset: Optional[Asset] = None

    @field_validator("duration", mode="before")
    @classmethod
    def _validate_duration(cls, v: Any) -> Any:
        if v is None:
            return v
        n = _try_int(v)
        if n is not None and (n < 0 or n > 500):
            raise ValueError("duration must be between 0 and 500")
        return v

    @model_validator(mode="before")
    @classmethod
    def _validate_meta_values(cls, values: Any) -> Any:
        if isinstance(values, dict):
            meta = values.get("meta")
            if isinstance(meta, dict):
                for k, v in meta.items():
                    if isinstance(v, str) and len(v) > 5000:
                        raise ValueError(f"meta value for '{k}' exceeds max length of 5000")
        return values


class RootPayload(APIModel):
    hasNext: Optional[bool] = None
    total: Optional[int] = None
    item: Optional[List[Item]] = None

    @field_validator("total", mode="before")
    @classmethod
    def _non_negative_total(cls, v: Any) -> Any:
        return _validate_non_negative(v)


def parse_payload(data: Dict[str, Any]) -> RootPayload:
    """Parse a decoded JSON dict into typed models."""
    return RootPayload.model_validate(data)


def parse_payload_strict_json(raw_json: str) -> Optional[RootPayload]:
    """
    Strict parser:
    - Payload too large -> PayloadTooLargeError
    - Invalid JSON -> JSONDecodeError
    - Type/schema issues -> ValidationError
    - Unexpected fields -> UnexpectedFieldError (with paths)
    - Known error responses (service error, not found) -> None
    """

    if not raw_json.strip():
        return None

    if len(raw_json.encode("utf-8")) > MAX_JSON_SIZE:
        raise PayloadTooLargeError(
            f"JSON payload exceeds maximum allowed size of {MAX_JSON_SIZE} bytes"
        )

    data = json.loads(raw_json)

    if not isinstance(data, dict):
        raise ValidationError.from_exception_data(
            "RootPayload",
            line_errors=[{"type": "dict_type", "input": data, "loc": ()}],
        )

    message = data.get("message")
    if message is not None:
        msg_lower = str(message).lower()
        if msg_lower == "service error":
            return None
        if "does not exist" in msg_lower:
            return None

    name = data.get("name")
    if name is not None and "notfound" in str(name).lower().replace(" ", ""):
        return None

    try:
        return RootPayload.model_validate_json(raw_json)
    except ValidationError as err:
        extras = _extract_extra_field_errors(err)
        if extras:
            details = "\n".join([f"- {path}: {msg}" for path, msg in extras])
            raise UnexpectedFieldError(
                "Unexpected field(s) encountered in upstream JSON; update models.\n"
                f"{details}"
            ) from err
        raise
    except json.JSONDecodeError as err:
        raise json.JSONDecodeError(
            f"Failed to parse upstream JSON: {err.msg}", err.doc, err.pos
        )
