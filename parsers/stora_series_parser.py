"""
Pydantic models for the PA Media JSON payload.

Notes
- Unknown/extra keys are forbidden
- Date-time fields are parsed as `datetime` when present.

2026
"""

from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, ConfigDict, Field, ValidationError
import json


class UnexpectedFieldError(ValueError):
    """Raised if a JSON contains unanticipated field"""

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
    """
    Base model: does not tolerate unexpected fields initally to find
    all variables in sample of JSON data.
    """
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Summary(APIModel):
    short: Optional[str] = None
    medium: Optional[str] = None
    long: Optional[str] = None
    welsh: Optional[str] = None
    supplemental: Optional[str] = None


class Category(APIModel):
    code: Optional[str] = None
    name: Optional[str] = None
    dvb: Optional[str] = None


class Subject(APIModel):
    code: Optional[str] = None
    profile: Optional[str] = None


class RenditionDefault(APIModel):
    width: Optional[int] = None
    height: Optional[int] = None
    href: Optional[str] = None


class Rendition(APIModel):
    default: Optional[RenditionDefault] = None


class Media(APIModel):
    kind: Optional[str] = None
    rendition: Optional[Rendition] = None
    copyright: Optional[str] = None
    expiry: Optional[datetime] = None


class Related(APIModel):
    id: Optional[str] = None
    title: Optional[str] = None
    type: Optional[str] = None
    number: Optional[int] = None
    subject: Optional[List[Subject]] = None
    media: Optional[List[Media]] = None


class Link(APIModel):
    rel: Optional[str] = None
    href: Optional[str] = None


class Contributor(APIModel):
    id: Optional[str] = None
    name: Optional[str] = None
    dob: Optional[date] = None
    from_: Optional[str] = Field(default=None, alias="from")
    gender: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    media: Optional[List[Media]] = None
    subject: Optional[List[Subject]] = None
    character: Optional[List[Any]] = None
    role: Optional[List[str]] = None


class Series(APIModel):
    id: str = None
    type: Optional[str] = None
    title: str = None
    name: Optional[str] = None
    message: Optional[str] = None
    runtime: Optional[int] = None
    keywords: Optional[List[Any]] = None
    mood: Optional[List[Any]] = None
    themes: Optional[List[Any]] = None
    soundtrack: Optional[List[Any]] = None
    locations: Optional[List[Any]] = None
    attribute: Optional[List[str]] = None
    category: Optional[List[Category]] = None
    contributor: Optional[List[Any]] = None
    certification: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None
    summary: Optional[Summary] = None
    media: Optional[List[Media]] = None
    related: Optional[List[Related]] = None
    subject: Optional[List[Subject]] = None
    link: Optional[List[Any]] = None
    deeplink: Optional[List[Any]] = None
    created_date: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_date: Optional[datetime] = Field(default=None, alias="updatedAt")
    deleted_date: Optional[datetime] = Field(default=None, alias="deletedAt")


def parse_payload(data: Dict[str, Any]) -> Series:
    """Parse a decoded JSON dict into typed models."""
    return Series.model_validate(data)


def parse_payload_strict_json(raw_json: str) -> Series:
    """
    Strict parser:
    - Invalid JSON -> JSONDecodeError
    - Type/schema issues -> ValidationError
    - Unexpected fields -> UnexpectedFieldError (with paths)
    """

    if not raw_json.strip():
        return None

    data = json.loads(raw_json)
    if "message" in data and data["message"] == "Service error":
        return None
    if "name" in data and data["name"] == "ResourceNotFoundError":
        return None

    try:
        return Series.model_validate_json(raw_json)
    except ValidationError as err:
        extras = _extract_extra_field_errors(err)
        if extras:
            details = "\n".join([f"- {path}: {msg}" for path, msg in extras])
            raise UnexpectedFieldError(
                "Unexpected field(s) encountered in upstream JSON; update models.\n"
                f"{details}"
            ) from err
        raise
    except json.JSONDecodeError:
        raise
