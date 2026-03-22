"""Tests for MAL models and validators."""

from datetime import date, datetime

from anibridge.providers.list.mal.models import Anime, MyAnimeListStatus


def test_my_anime_list_status_parses_dates_and_tags() -> None:
    """MyAnimeListStatus should normalize date and tag inputs."""
    status = MyAnimeListStatus.model_validate(
        {
            "status": "watching",
            "start_date": "2024-02",
            "finish_date": datetime(2024, 2, 20, 8, 30, 0),
            "updated_at": "2024-02-21T10:30:00",
            "tags": "fav,recommend,",
        }
    )

    assert status.start_date == date(2024, 2, 1)
    assert status.finish_date == date(2024, 2, 20)
    assert status.updated_at is not None
    assert status.tags == ["fav", "recommend"]


def test_my_anime_list_status_handles_invalid_dates() -> None:
    """MyAnimeListStatus should gracefully handle invalid date-like values."""
    status = MyAnimeListStatus.model_validate(
        {
            "status": "watching",
            "start_date": "not-a-date",
            "finish_date": "2024-13-80",
            "tags": None,
        }
    )

    assert status.start_date is None
    assert status.finish_date is None
    assert status.tags == []


def test_anime_parses_temporal_fields() -> None:
    """Anime should parse date and datetime values from strings."""
    anime = Anime.model_validate(
        {
            "id": 10,
            "title": "Temporal",
            "start_date": "2024-03",
            "end_date": "2024",
            "created_at": "2024-03-01T12:00:00",
            "updated_at": datetime(2024, 3, 1, 13, 0, 0),
        }
    )

    assert anime.start_date == date(2024, 3, 1)
    assert anime.end_date == date(2024, 1, 1)
    assert anime.created_at is not None
    assert anime.updated_at is not None


def test_anime_ignores_unknown_fields() -> None:
    """MalBaseModel config should ignore unknown payload keys."""
    anime = Anime.model_validate(
        {
            "id": 11,
            "title": "Unknown Keys",
            "start_date": "2024-01-01",
            "unknown_field": "ignored",
            "another": {"value": 1},
        }
    )

    assert anime.id == 11
    assert anime.title == "Unknown Keys"
    assert anime.start_date == date(2024, 1, 1)
