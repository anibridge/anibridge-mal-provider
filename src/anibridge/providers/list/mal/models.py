"""Models for the MAL API."""

import contextlib
from datetime import date, datetime
from enum import StrEnum
from typing import Any

import msgspec


def _parse_date(value: Any) -> date | None | Any:
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
    except ValueError, IndexError:
        return None


def _parse_datetime(value: Any) -> datetime | None | Any:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(str(value))
    return value


def _split_tags(value: Any) -> list[str] | Any:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [tag for tag in str(value).split(",") if tag]


class MalBaseModel(msgspec.Struct, kw_only=True):
    """Base model for MAL responses."""


class Picture(MalBaseModel):
    """Picture resource returned by MAL."""

    large: str | None = None
    medium: str | None = None


class AlternativeTitles(MalBaseModel):
    """Alternative titles for an anime."""

    synonyms: list[str] = msgspec.field(default_factory=list)
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

    status: MalListStatus | None = None
    score: int | None = None
    num_episodes_watched: int | None = None
    is_rewatching: bool | None = None
    start_date: Any = None
    finish_date: Any = None
    priority: int | None = None
    num_times_rewatched: int | None = None
    rewatch_value: int | None = None
    tags: Any = msgspec.field(default_factory=list)
    comments: str | None = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        """Normalize MAL status fields after decoding."""
        self.start_date = _parse_date(self.start_date)
        self.finish_date = _parse_date(self.finish_date)
        self.updated_at = _parse_datetime(self.updated_at)
        self.tags = _split_tags(self.tags)


class Anime(MalBaseModel):
    """Anime resource as returned by MAL."""

    id: int
    title: str
    main_picture: Picture | None = None
    alternative_titles: AlternativeTitles | None = None
    start_date: Any = None
    end_date: Any = None
    synopsis: str | None = None
    mean: float | None = None
    rank: int | None = None
    popularity: int | None = None
    num_list_users: int | None = None
    num_scoring_users: int | None = None
    nsfw: str | None = None
    genres: list[Genre] = msgspec.field(default_factory=list)
    created_at: Any = None
    updated_at: Any = None
    media_type: str | None = None
    status: str | None = None
    my_list_status: MyAnimeListStatus | None = None
    num_episodes: int | None = None
    start_season: Season | None = None
    broadcast: Broadcast | None = None
    source: str | None = None
    average_episode_duration: int | None = None
    rating: str | None = None

    def __post_init__(self) -> None:
        """Normalize MAL anime fields after decoding."""
        self.start_date = _parse_date(self.start_date)
        self.end_date = _parse_date(self.end_date)
        self.created_at = _parse_datetime(self.created_at)
        self.updated_at = _parse_datetime(self.updated_at)


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

    data: list[AnimePagingData] = msgspec.field(default_factory=list)
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
