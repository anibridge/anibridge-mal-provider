"""Tests for the MAL list provider implementation."""

import json
from datetime import UTC, date, datetime

import pytest
from anibridge.list import ListStatus

from anibridge.providers.list.mal.list import (
    MalListEntry,
    _list_status_to_mal,
    _mal_status_to_list,
)
from anibridge.providers.list.mal.models import Anime, MalListStatus, MyAnimeListStatus


def test_status_mapping_helpers() -> None:
    """Ensure status mapping helpers convert between AniBridge and MAL enums."""
    assert _mal_status_to_list(MalListStatus.WATCHING) is ListStatus.CURRENT
    assert _mal_status_to_list(MalListStatus.COMPLETED) is ListStatus.COMPLETED
    assert _mal_status_to_list(MalListStatus.PLAN_TO_WATCH) is ListStatus.PLANNING
    assert _mal_status_to_list("unknown") is None

    status, rewatching = _list_status_to_mal(ListStatus.REPEATING)
    assert status is MalListStatus.WATCHING
    assert rewatching is True
    status, rewatching = _list_status_to_mal(ListStatus.DROPPED)
    assert status is MalListStatus.DROPPED
    assert rewatching is False
    assert _list_status_to_mal(None) == (None, False)


@pytest.mark.asyncio
async def test_entry_validations_and_conversion(mal_provider, fake_client) -> None:
    """MalListEntry should scale ratings and reject invalid input."""
    anime = Anime(
        id=42,
        title="Test Show",
        num_episodes=12,
        media_type="tv",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.WATCHING,
            num_episodes_watched=3,
            score=7,
        ),
    )
    fake_client.offline_anime_entries[anime.id] = anime

    entry = MalListEntry(mal_provider, anime)
    assert entry.status is ListStatus.CURRENT
    assert entry.user_rating == 70

    entry.user_rating = 95
    assert entry.user_rating == 100

    entry.progress = 5
    assert entry.progress == 5
    entry.repeats = 2
    assert entry.repeats == 2

    entry.started_at = datetime(2024, 1, 1, tzinfo=UTC)
    entry.finished_at = datetime(2024, 1, 10, tzinfo=UTC)
    assert entry.started_at is not None and entry.started_at.date() == date(2024, 1, 1)
    assert entry.finished_at is not None and entry.finished_at.date() == date(
        2024, 1, 10
    )

    with pytest.raises(ValueError):
        entry.user_rating = 150
    with pytest.raises(ValueError):
        entry.progress = -1
    with pytest.raises(ValueError):
        entry.repeats = -1


@pytest.mark.asyncio
async def test_backup_and_restore_round_trip(mal_provider, fake_client) -> None:
    """Backup produces JSON and restore replays updates through the client stub."""
    anime_one = Anime(
        id=1,
        title="Alpha",
        num_episodes=24,
        media_type="tv",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.WATCHING,
            num_episodes_watched=12,
            score=6,
            start_date=date(2023, 1, 1),
            finish_date=None,
            tags=["action", "winter"],
        ),
    )
    anime_two = Anime(
        id=2,
        title="Beta",
        num_episodes=1,
        media_type="movie",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.COMPLETED,
            num_episodes_watched=1,
            score=8,
            start_date=date(2022, 6, 1),
            finish_date=date(2022, 6, 2),
            comments="Great",
        ),
    )
    fake_client.offline_anime_entries = {
        anime_one.id: anime_one,
        anime_two.id: anime_two,
    }

    backup = await mal_provider.backup_list()
    payload = json.loads(backup)
    assert {item["id"] for item in payload} == {1, 2}
    assert any(item["status"] == MalListStatus.WATCHING for item in payload)

    fake_client.update_calls.clear()
    await mal_provider.restore_list(backup)

    assert len(fake_client.update_calls) == 2
    first_call = fake_client.update_calls[0]
    assert first_call["progress"] in {1, 12}
    assert first_call["status"] in {MalListStatus.WATCHING, MalListStatus.COMPLETED}
    assert isinstance(first_call["start_date"], (date, type(None)))
    assert "comments" in first_call
