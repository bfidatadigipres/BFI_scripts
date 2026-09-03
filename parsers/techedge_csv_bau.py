"""
Pydantic models for the TechEdge CSV data (raw not cleaned data)

Notes
- Unknown/extra keys are forbidden
- Date-time fields are parsed as `datetime` when present.
- All string fields have length limits to prevent buffer/log injection
- CSV injection prefixes are detected and rejected
- File paths are validated against allowed directory

2026
"""

from pathlib import Path
from typing import Annotated
import re
from pydantic import BaseModel, Field, ConfigDict
from pydantic.functional_validators import BeforeValidator
from pydantic_csv import BasemodelCSVReader

CHNL = {
    "5STAR",
    "CH4",
    "5",
    "Channel 5",
    "E4",
    "Film4",
    "ITV1",
    "ITV1 HD",
    "ITV2",
    "ITV3",
    "ITV4",
    "ITVBe",
    "ITVQuiz",
    "More4",
}

_ALLOWED_DIR_NAME = "adverts_techedge_no_dupes"
_MAX_ERROR_VALUE_LEN = 50
_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_CSV_INJECTION_PREFIXES = ("=", "+", "@")


def _safe_repr(v: object) -> str:
    s = repr(v)
    if len(s) > _MAX_ERROR_VALUE_LEN:
        return s[:_MAX_ERROR_VALUE_LEN] + "..."
    return s


def sanitize_csv_text(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if v and v[0] in _CSV_INJECTION_PREFIXES:
        raise ValueError("Potential CSV injection detected in field value")
    if not all(c.isprintable() for c in v):
        raise ValueError("Non-printable characters detected in field value")
    return v


def validate_date_format(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if not _DATE_RE.match(v):
        raise ValueError(f"Invalid date format (expected DD/MM/YYYY): {_safe_repr(v)}")
    return v


def validate_time_format(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if not _TIME_RE.match(v):
        raise ValueError(f"Invalid time format (expected HH:MM:SS): {_safe_repr(v)}")
    return v


def parse_channel(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if v not in CHNL:
        raise ValueError(f"Invalid channel: {_safe_repr(v)}")
    return v


def parse_film_code(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if len(v) != 13:
        raise ValueError(f"Invalid length for Film Code: {_safe_repr(v)}")
    if not v.isalnum():
        raise ValueError(f"Illegal characters in Film Code: {_safe_repr(v)}")
    return v.upper()


def parse_impacts_pos(v):
    if v is None or v == "":
        return None
    try:
        v = int(v)
    except (ValueError, TypeError):
        return None
    if v < 0 or v > 99:
        raise ValueError(f"Invalid length/range for number: {_safe_repr(v)}")
    return v


SanitizedStr = Annotated[str | None, BeforeValidator(sanitize_csv_text)]
DateStr = Annotated[str | None, BeforeValidator(validate_date_format)]
TimeStr = Annotated[str | None, BeforeValidator(validate_time_format)]
ChannelStr = Annotated[str | None, BeforeValidator(parse_channel)]
FilmCodeStr = Annotated[str | None, BeforeValidator(parse_film_code)]
ImpactsPosInt = Annotated[int | None, BeforeValidator(parse_impacts_pos)]


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel: ChannelStr = Field(default=None, alias="Channel", max_length=20)
    date: DateStr = Field(default=None, alias="Date", max_length=10)
    start_time: TimeStr = Field(default=None, alias="Start time", max_length=8)
    film_code: FilmCodeStr = Field(default=None, alias="Film Code", max_length=13)
    break_code: SanitizedStr = Field(default=None, alias="Break Code", max_length=4)
    advertiser: SanitizedStr = Field(default=None, alias="Advertiser", max_length=100)
    brand: SanitizedStr = Field(default=None, alias="Brand", max_length=100)
    agency: SanitizedStr = Field(default=None, alias="Agency", max_length=100)
    hold_comp: SanitizedStr = Field(default=None, alias="Holding Company", max_length=100)
    barb_before: SanitizedStr = Field(default=None, alias="BARB Prog Before", max_length=100)
    barb_after: SanitizedStr = Field(default=None, alias="BARB Prog After", max_length=100)
    sales_house: SanitizedStr = Field(default=None, alias="Sales House", max_length=100)
    major_category: SanitizedStr = Field(default=None, alias="Major category", max_length=150)
    mid_category: SanitizedStr = Field(default=None, alias="Mid category", max_length=150)
    minor_category: SanitizedStr = Field(default=None, alias="Minor category", max_length=150)
    pib_rel: SanitizedStr = Field(default=None, alias="All PIB rel", max_length=100)
    pib_pos: ImpactsPosInt = Field(default=None, alias="All PIB pos")
    log_station: SanitizedStr = Field(default=None, alias="Log Station (2010-)", max_length=100)
    impacts: SanitizedStr = Field(default=None, alias="Impacts A4+", max_length=100)


def iter_techedge_rows(csv_path: str, max_rows: int = 10000):
    """
    Iterate rows and validate with BaseModel above.

    Args:
        csv_path: Path to the CSV file. Must be within an allowed directory
                  containing 'adverts_techedge_no_dupes' and have .csv extension.
        max_rows: Maximum number of rows to yield (prevents DoS). Default 10000.
    """
    path = Path(csv_path).resolve()

    if path.suffix.lower() != ".csv":
        raise ValueError(f"Not a CSV file: {csv_path}")
    if not path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    parts = path.parts
    if _ALLOWED_DIR_NAME not in parts:
        raise ValueError(
            f"Path must be within '{_ALLOWED_DIR_NAME}' directory: {csv_path}"
        )

    with open(path, "r", encoding="utf-8", newline="") as f:
        for count, row in enumerate(BasemodelCSVReader(f, Data), start=1):
            if count > max_rows:
                break
            yield row
