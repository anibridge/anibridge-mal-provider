"""Models for the MAL API."""

import contextlib
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MalBaseModel(BaseModel):
    """Base model for MAL responses."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class Picture(MalBaseModel):
    """Picture resource returned by MAL."""

    large: str | None = None
    medium: str | None = None


class AlternativeTitles(MalBaseModel):
    """Alternative titles for an anime."""

    synonyms: list[str] = Field(default_factory=list)
    en: str | None = None
    ja: str | None = None


class Genre(MalBaseModel):
    """Genre resource returned by MAL."""

    id: int | None = None
    name: str | None = None


class Season(MalBaseModel):
    """Season information for an anime."""

    year: int | None = None
    season: str | None = None


class Broadcast(MalBaseModel):
    """Broadcast information for an anime."""

    day_of_the_week: str | None = None
    start_time: str | None = None


class MalListStatus(StrEnum):
    """Status values accepted by MAL."""

    WATCHING = "watching"
    COMPLETED = "completed"
    ON_HOLD = "on_hold"
    DROPPED = "dropped"
    PLAN_TO_WATCH = "plan_to_watch"


class MyAnimeListStatus(MalBaseModel):
    """User-specific list status returned by MAL."""

    status: MalListStatus | str | None = None
    score: int | None = None
    num_episodes_watched: int | None = None
    is_rewatching: bool | None = None
    start_date: date | None = None
    finish_date: date | None = None
    priority: int | None = None
    num_times_rewatched: int | None = None
    rewatch_value: int | None = None
    tags: list[str] = Field(default_factory=list)
    comments: str | None = None
    updated_at: datetime | None = None

    @field_validator("start_date", "finish_date", mode="before")
    @classmethod
    def _parse_date(cls, value: Any) -> date | None | Any:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if not isinstance(value, str):
            return value
        with contextlib.suppress(ValueError):
            return date.fromisoformat(str(value))

        parts = value.split("-")
        try:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1
            return date(year, month, day)
        except (ValueError, IndexError):
            return None

    @field_validator("updated_at", mode="before")
    @classmethod
    def _parse_datetime(cls, value: Any) -> datetime | Any:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        with contextlib.suppress(ValueError):
            return datetime.fromisoformat(str(value))
        return value

    @field_validator("tags", mode="before")
    @classmethod
    def _split_tags(cls, value: Any) -> list[str] | Any:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [tag for tag in str(value).split(",") if tag]


class Anime(MalBaseModel):
    """Anime resource as returned by MAL."""

    id: int
    title: str
    main_picture: Picture | None = None
    alternative_titles: AlternativeTitles | None = None
    start_date: date | None = None
    end_date: date | None = None
    synopsis: str | None = None
    mean: float | None = None
    rank: int | None = None
    popularity: int | None = None
    num_list_users: int | None = None
    num_scoring_users: int | None = None
    nsfw: str | None = None
    genres: list[Genre] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    media_type: str | None = None
    status: str | None = None
    my_list_status: MyAnimeListStatus | None = None
    num_episodes: int | None = None
    start_season: Season | None = None
    broadcast: Broadcast | None = None
    source: str | None = None
    average_episode_duration: int | None = None
    rating: str | None = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _parse_date(cls, value: Any) -> date | None | Any:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if not isinstance(value, str):
            return value
        with contextlib.suppress(ValueError):
            return date.fromisoformat(str(value))

        parts = value.split("-")
        try:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1
            return date(year, month, day)
        except (ValueError, IndexError):
            return None

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def _parse_datetime(cls, value: Any) -> datetime | Any:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        with contextlib.suppress(ValueError):
            return datetime.fromisoformat(str(value))
        return value


class AnimePagingData(MalBaseModel):
    """Anime data returned in paginated responses."""

    node: Anime
    list_status: MyAnimeListStatus | None = None


class Paging(MalBaseModel):
    """Paging information for paginated responses."""

    previous: str | None = None
    next: str | None = None


class AnimePaging(MalBaseModel):
    """Paginated anime response from MAL."""

    data: list[AnimePagingData] = Field(default_factory=list)
    paging: Paging | None = None


class User(MalBaseModel):
    """User resource returned by MAL."""

    id: int
    name: str
    picture: str | None = None
    gender: str | None = None
    birthday: str | None = None
    location: str | None = None
    time_zone: str | None = None
