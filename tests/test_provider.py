"""Tests for the MAL provider contract."""

from datetime import UTC, date, datetime
from logging import getLogger
from typing import Any, cast

import pytest
from anibridge.provider.base import (
    RecordField,
    RecordQuery,
    Ref,
    State,
    Status,
    UpsertRecord,
)

from anibridge.providers.mal.client import MalClient
from anibridge.providers.mal.models import Anime, MalListStatus, MyAnimeListStatus
from anibridge.providers.mal.provider import MalProvider


@pytest.fixture()
def provider(fake_client: Any) -> MalProvider:
    """Return a MAL provider wired to the fake client."""
    provider = MalProvider(
        logger=getLogger("tests.provider"),
        config={"token": "fake-token", "client_id": "fake-client-id"},
    )
    provider._client = cast(MalClient, fake_client)
    return provider


def test_record_from_anime_preserves_mal_date_precision(provider: MalProvider) -> None:
    """MAL date fields should remain dates in normalized records."""
    anime = Anime(
        id=101,
        title="Cowboy Bebop",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.COMPLETED,
            start_date=date(2026, 1, 2),
            finish_date=date(2026, 1, 3),
        ),
    )

    record = provider._record_from_anime(anime, frozenset())

    assert record.values[RecordField.STARTED_AT] == date(2026, 1, 2)
    assert record.values[RecordField.FINISHED_AT] == date(2026, 1, 3)


@pytest.mark.asyncio()
async def test_fetch_records_returns_existing_mal_list_state(
    provider: MalProvider,
    fake_client: Any,
) -> None:
    """MAL target record reads should return fetched list state."""
    fake_client.offline_anime_entries[101] = Anime(
        id=101,
        title="Cowboy Bebop",
        my_list_status=MyAnimeListStatus(
            status=MalListStatus.WATCHING,
            score=8,
            num_episodes_watched=4,
        ),
    )

    page = await provider.fetch_records(
        RecordQuery(refs=(Ref.anchor("101"),), record_surfaces=("anime_list",))
    )

    assert len(page.items) == 1
    record = page.items[0]
    assert record.ref == Ref.anchor("101")
    assert record.values[RecordField.STATUS] == State(
        native="watching",
        status=Status.ACTIVE,
    )
    assert record.values[RecordField.RATING].value == 8
    assert record.values[RecordField.PROGRESS].current == 4


def test_capabilities_include_rewatch_status(provider: MalProvider) -> None:
    """MAL rewatch support shares MAL's watching native value."""
    user_state = next(record for record in provider.capabilities().records)
    status_spec = user_state.fields[RecordField.STATUS]
    native_by_semantic = {
        descriptor.semantic: descriptor.native for descriptor in status_spec.values
    }

    assert native_by_semantic[Status.ACTIVE] == "watching"
    assert native_by_semantic[Status.REPEATING] == "watching"


@pytest.mark.asyncio()
async def test_repeating_status_writes_mal_rewatching(
    provider: MalProvider,
    fake_client: Any,
) -> None:
    """A repeating write uses MAL's watching status plus rewatching flag."""
    fake_client.offline_anime_entries[101] = Anime(id=101, title="Cowboy Bebop")

    results = await provider.write_records(
        (
            UpsertRecord(
                ref=Ref.anchor("101"),
                surface="anime_list",
                set={
                    RecordField.STATUS: State(
                        native="watching",
                        status=Status.REPEATING,
                    )
                },
            ),
        )
    )

    assert results[0].ok
    assert fake_client.update_calls[0]["status"] is MalListStatus.WATCHING
    assert fake_client.update_calls[0]["is_rewatching"] is True


def test_date_value_accepts_dates_for_date_precision(provider: MalProvider) -> None:
    """MAL writes should accept dates and still tolerate aware datetimes."""
    assert provider._date_value(
        RecordField.STARTED_AT,
        date(2026, 1, 2),
    ) == date(2026, 1, 2)
    assert provider._date_value(
        RecordField.FINISHED_AT,
        datetime(2026, 1, 3, 12, 30, tzinfo=UTC),
    ) == date(2026, 1, 3)

    with pytest.raises(ValueError, match="timezone-aware"):
        provider._date_value(
            RecordField.STARTED_AT,
            datetime(2026, 1, 2),
        )
