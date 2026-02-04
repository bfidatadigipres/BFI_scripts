"""
Pydantic models for the PA Media JSON payload.

Notes
- Unknown/extra keys are forbidden
- Date-time fields are parsed as `datetime` when present.

2026
"""

from __future__ import annotations
from datetime import date, datetime
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
    # model_config = ConfigDict(extra="allow", populate_by_name=True)
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Item(APIModel):
    id: Optional[str] = None
    name: Optional[str] = None
    dob: Optional[date] = None
    dod: Optional[date] = None
    from_: Optional[str] = Field(default=None, alias="from")
    gender: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    media: Optional[List[Any]] = None
    created_date: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_date: Optional[datetime] = Field(default=None, alias="updatedAt")
    character: Optional[List[Any]] = None
    role: Optional[List[str]] = None


class RootPayload(APIModel):
    name: Optional[str] = None
    message: Optional[str] = None
    hasNext: Optional[bool] = None
    total: Optional[int] = None
    item: Optional[List[Item]] = None


def parse_payload(data: Dict[str, Any]) -> RootPayload:
    """Parse a decoded JSON dict into typed models."""
    return RootPayload.model_validate(data)


def parse_payload_strict_json(raw_json: str) -> RootPayload:
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
    elif "message" in data and "does not exist." in data["message"]:
        return None
    if "name" in data and "NotFound" in data["name"]:
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
    except json.JSONDecodeError:
        raise

