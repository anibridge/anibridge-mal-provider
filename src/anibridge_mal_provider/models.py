"""MyAnimeList models and enums used by the AniBridge MAL provider."""

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

UTCDateTime = Annotated[datetime, AfterValidator(lambda dt: dt.astimezone(UTC))]


class MalMediaFormat(StrEnum):
    """Subset of media types returned by the MAL API we care about."""

    UNKNOWN = "unknown"
    TV = "tv"
    OVA = "ova"
    MOVIE = "movie"
    SPECIAL = "special"
    ONA = "ona"
    MUSIC = "music"


class MalMediaStatus(StrEnum):
    """Lifecycle status of a MAL anime entry."""

    FINISHED = "finished_airing"
    CURRENT = "currently_airing"
    NOT_YET_RELEASED = "not_yet_aired"


class MalListEntryStatus(StrEnum):
    """Status values supported by MAL watch lists."""

    WATCHING = "watching"
    COMPLETED = "completed"
    ON_HOLD = "on_hold"
    DROPPED = "dropped"
    PLAN_TO_WATCH = "plan_to_watch"


class MalNsfwLevel(StrEnum):
    """NSFW levels defined by MAL."""

    WHITE = "white"
    GRAY = "gray"
    BLACK = "black"


class MalSeason(StrEnum):
    """Seasons defined by MAL."""

    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    FALL = "fall"


class MalBaseModel(BaseModel):
    """Base model that configures common serialization behavior."""

    model_config = ConfigDict(populate_by_name=True)


class Picture(MalBaseModel):
    """Representation of MAL cover/thumbnail imagery."""

    medium: str | None = None
    large: str | None = None


class AlternativeTitles(MalBaseModel):
    """Container for localized MAL titles."""

    synonyms: list[str] = Field(default_factory=list)
    en: str | None = None
    ja: str | None = None


class MalUserStatistics(MalBaseModel):
    """Basic MAL statistics for the authenticated user."""

    num_days_watched: float | None = None
    num_episodes: int | None = None


class MalGenre(MalBaseModel):
    """Representation of a MAL genre."""

    id: int
    name: str


class MalSeasonYear(MalBaseModel):
    """Representation of a MAL season and year."""

    season: MalSeason
    year: int


class MalUser(MalBaseModel):
    """Representation of the authenticated MAL user."""

    id: int
    name: str
    picture: str | None = None
    anime_statistics: MalUserStatistics | None = None


class MalListStatus(MalBaseModel):
    """Raw MAL list status payload returned for anime entries."""

    status: MalListEntryStatus | None = None
    score: int = 0
    num_episodes_watched: int = 0
    is_rewatching: bool = False
    start_date: date | None = None
    finish_date: date | None = None
    priority: int = 0
    num_times_rewatched: int = 0
    rewatch_value: int = 0
    tags: list[str] = Field(default_factory=list)
    comments: str = ""
    updated_at: UTCDateTime | None = None


class MalMedia(MalBaseModel):
    """Slim MAL anime payload used by the provider."""

    id: int
    title: str
    main_picture: Picture | None = None
    alternative_titles: AlternativeTitles | None = None
    start_date: date | None = None
    end_date: date | None = None
    # synopsis: str | None = None
    # mean: float | None = None
    # rank: int | None = None
    # popularity: int | None = None
    # num_list_users: int = 0
    # num_scoring_users: int = 0
    nsfw: MalNsfwLevel | None = None
    # genres: list[MalGenre] = Field(default_factory=list)
    created_at: UTCDateTime | None = None
    updated_at: UTCDateTime | None = None
    media_type: MalMediaFormat | None = None
    status: MalMediaStatus | None = None
    my_list_status: MalListStatus | None = None
    num_episodes: int = 0
    start_season: MalSeasonYear | None = None
    # broadcast
    # source
    average_episode_duration: int | None = None  # in seconds
    # rating: str | None = None
    # studios
    pictures: list[Picture] = Field(default_factory=list)
    # background: str | None = None
    # related_anime
    # related_manga
    # recommendations
    # statistics
