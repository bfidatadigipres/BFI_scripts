"""
Pydantic models for the TechEdge CSV data (raw not cleaned data)

Notes
- Unknown/extra keys are forbidden
- Date-time fields are parsed as `datetime` when present.

2026
"""

from typing import Annotated
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


def parse_channel(v):
    if v is None or v == "":
        return None
    if v not in CHNL:
        raise ValueError(f"Invalid channel: {v!r}")
    return v


def parse_film_code(v):
    if v is None or v == "":
        return None
    v = str(v).strip()
    if len(v) != 13:
        raise ValueError(f"Invalid length for Film Code: {v!r}")
    if not v.isalnum():
        raise ValueError(f"Illegal characters in Film Code: {v!r}")
    return v.upper()


def parse_impacts_pos(v):
    if v is None or v == "":
        return None
    v = int(v)
    if v < 0 or v > 99:
        raise ValueError(f"Invalid length/range for number: {v!r}")
    return v


ChannelStr = Annotated[str | None, BeforeValidator(parse_channel)]
FilmCodeStr = Annotated[str | None, BeforeValidator(parse_film_code)]
ImpactsPosInt = Annotated[int | None, BeforeValidator(parse_impacts_pos)]


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel: ChannelStr = Field(default=None, alias="Channel")
    date: str | None = Field(default=None, alias="Date")  # consider datetime/date later
    start_time: str | None = Field(default=None, alias="Start time")
    film_code: FilmCodeStr = Field(default=None, alias="Film Code")
    advertiser: str | None = Field(default=None, alias="Advertiser")
    brand: str | None = Field(default=None, alias="Brand")
    agency: str | None = Field(default=None, alias="Agency")
    hold_comp: str | None = Field(default=None, alias="Holding Company")
    barb_before: str | None = Field(default=None, alias="BARB Prog Before")
    barb_after: str | None = Field(default=None, alias="BARB Prog After")
    major_category: str | None = Field(default=None, alias="Major category")
    mid_category: str | None = Field(default=None, alias="Mid category")
    minor_category: str | None = Field(default=None, alias="Minor category")
    pib_pos: ImpactsPosInt = Field(default=None, alias="All PIB pos")
    original: str | None = Field(default=None, alias="Original values")
    datestamp: str |  None = Field(default=None, alias="datetime")


def iter_techedge_rows(csv_path: str):
    """
    Iterate rows and validate
    with BaseModel above
    """
    with open(csv_path, "r", encoding="latin1", newline="") as f:
        yield from BasemodelCSVReader(f, Data)
