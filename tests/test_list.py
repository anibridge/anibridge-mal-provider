"""Tests for the MAL list provider implementation."""

import json
import logging
from copy import copy
from datetime import UTC, date, datetime
from typing import cast

import pytest
from anibridge.list import ListStatus
from anibridge.utils.types import ProviderLogger

from anibridge.providers.list.mal.list import (
    MalListEntry,
    MalListMedia,
    MalListProvider,
    _list_status_to_mal,
    _mal_status_to_list,
)
from anibridge.providers.list.mal.models import (
    Anime,
    MalListStatus,
    MyAnimeListStatus,
    Picture,
    Season,
)


def test_status_mapping_helpers() -> None:
    """Ensure status mapping helpers convert between AniBridge and MAL enums."""
    assert _mal_status_to_list(MalListStatus.WATCHING) is ListStatus.CURRENT
    assert (
        _mal_status_to_list(MalListStatus.WATCHING, is_rewatching=True)
        is ListStatus.REPEATING
    )
    assert _mal_status_to_list(MalListStatus.COMPLETED) is ListStatus.COMPLETED
    assert _mal_status_to_list(MalListStatus.PLAN_TO_WATCH) is ListStatus.PLANNING
    assert _mal_status_to_list("unknown") is None

    status, rewatching = _list_status_to_mal(ListStatus.REPEATING)
    assert status is MalListStatus.WATCHING
    assert rewatching is True
    status, rewatching = _list_status_to_mal(ListStatus.DROPPED)
    assert status is MalListStatus.DROPPED
    assert rewatching is False
    status, rewatching = _list_status_to_mal(ListStatus.PAUSED)
    assert status is MalListStatus.ON_HOLD
    assert rewatching is False
    status, rewatching = _list_status_to_mal(ListStatus.PLANNING)
    assert status is MalListStatus.PLAN_TO_WATCH
    assert rewatching is False
    assert _list_status_to_mal(None) == (None, False)


def test_media_metadata_helpers(mal_provider) -> None:
    """MalListMedia should expose normalized labels, urls, and media attributes."""
    anime = Anime(
        id=501,
        title="Media Test",
        media_type="movie",
        status="finished_airing",
        start_season=Season(year=2024, season="spring"),
        main_picture=Picture(large=None, medium="https://img.medium"),
        num_episodes=None,
    )
    media = MalListMedia(provider=mal_provider, anime=anime)  # type: ignore[arg-type]

    assert media.external_url == "https://myanimelist.net/anime/501"
    assert media.labels == ["Spring 2024", "Movie", "Finished Airing"]
    assert media.total_units == 1
    assert media.poster_image == "https://img.medium"


def test_provider_default_rate_limit_uses_global_limiter() -> None:
    """When rate_limit is omitted, provider should keep it as None."""
    provider = MalListProvider(
        logger=cast(ProviderLogger, logging.getLogger("tests.list")),
        config={"token": "abc"},
    )

    assert provider.parsed_config.rate_limit is None
    assert provider._client.rate_limit is None


def test_provider_passes_custom_rate_limit_to_client() -> None:
    """Configured rate_limit should be forwarded to the MAL client."""
    provider = MalListProvider(
        logger=cast(ProviderLogger, logging.getLogger("tests.list")),
        config={"token": "abc", "rate_limit": 90},
    )

    assert provider.parsed_config.rate_limit == 90
    assert provider._client.rate_limit == 90


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
async def test_progress_update_does_not_resubmit_comments(
    mal_provider, fake_client
) -> None:
    """Progress-only updates should preserve and omit existing MAL comments."""
    comments = "line 1\nline 2<br />line 3"
    anime = Anime(
        id=43,
        title="Commented Show",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.WATCHING,
            num_episodes_watched=1,
            tags=["source"],
            comments=comments,
        ),
    )
    fake_client.offline_anime_entries[anime.id] = anime
    source_entry = MalListEntry(mal_provider, anime)
    planned_entry = copy(source_entry)

    planned_entry.progress = 2
    planned_entry._status.tags.append("planned")
    await mal_provider.update_entry(planned_entry.media().key, planned_entry)

    assert source_entry.progress == 1
    assert source_entry.review == comments
    assert source_entry._status.tags == ["source"]
    assert planned_entry.review == comments
    assert fake_client.update_calls[-1]["comments"] is None
    assert fake_client.update_calls[-1]["tags"] == []


@pytest.mark.asyncio
async def test_repeating_update_sends_mal_rewatch_flag(
    mal_provider, fake_client
) -> None:
    """Repeating updates should send MAL's watching status and rewatch flag."""
    anime = Anime(
        id=44,
        title="Repeating Show",
        my_list_status=MyAnimeListStatus(status=MalListStatus.WATCHING),
    )
    fake_client.offline_anime_entries[anime.id] = anime
    source_entry = MalListEntry(mal_provider, anime)
    planned_entry = copy(source_entry)

    planned_entry.status = ListStatus.REPEATING
    await mal_provider.update_entry(planned_entry.media().key, planned_entry)

    update = fake_client.update_calls[-1]
    assert update["status"] is MalListStatus.WATCHING
    assert update["is_rewatching"] is True
    assert update["comments"] is None
    assert source_entry.status is ListStatus.CURRENT
    assert planned_entry.status is ListStatus.REPEATING


@pytest.mark.asyncio
async def test_update_entry_skips_unchanged_entry(mal_provider, fake_client) -> None:
    """Unmodified entries should not trigger an empty MAL PATCH request."""
    anime = Anime(
        id=45,
        title="Unchanged Show",
        my_list_status=MyAnimeListStatus(status=MalListStatus.PLAN_TO_WATCH),
    )
    entry = MalListEntry(mal_provider, anime)

    await mal_provider.update_entry(entry.media().key, entry)

    assert fake_client.update_calls == []


@pytest.mark.asyncio
async def test_batch_updates_keep_independent_field_masks(
    mal_provider, fake_client
) -> None:
    """Sequential MAL batch updates should not leak fields between entries."""
    progress_anime = Anime(
        id=46,
        title="Progress Show",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.WATCHING,
            num_episodes_watched=1,
            comments="keep",
        ),
    )
    review_anime = Anime(
        id=47,
        title="Review Show",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.COMPLETED,
            num_episodes_watched=12,
            comments="old",
        ),
    )
    fake_client.offline_anime_entries = {
        progress_anime.id: progress_anime,
        review_anime.id: review_anime,
    }
    progress_entry = copy(MalListEntry(mal_provider, progress_anime))
    review_entry = copy(MalListEntry(mal_provider, review_anime))
    progress_entry.progress = 2
    review_entry.review = "new"

    await mal_provider.update_entries_batch([progress_entry, review_entry])

    progress_update, review_update = fake_client.update_calls
    assert progress_update["progress"] == 2
    assert progress_update["comments"] is None
    assert review_update["progress"] is None
    assert review_update["comments"] == "new"


@pytest.mark.asyncio
async def test_backup_and_restore_round_trip(mal_provider, fake_client) -> None:
    """Backup produces JSON and restore replays updates and removes stale entries."""
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
    anime_three = Anime(
        id=3,
        title="Gamma",
        num_episodes=13,
        media_type="tv",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.DROPPED,
            num_episodes_watched=4,
        ),
    )
    fake_client.offline_anime_entries[anime_three.id] = anime_three

    await mal_provider.restore_list(backup)

    assert len(fake_client.update_calls) == 2
    first_call = fake_client.update_calls[0]
    assert first_call["progress"] in {1, 12}
    assert first_call["status"] in {MalListStatus.WATCHING, MalListStatus.COMPLETED}
    assert isinstance(first_call["start_date"], (date, type(None)))
    assert "comments" in first_call
    assert fake_client.deleted_ids == [3]
    assert set(fake_client.offline_anime_entries) == {1, 2}
