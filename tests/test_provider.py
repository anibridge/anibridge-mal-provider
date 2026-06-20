"""Tests for the MAL provider contract."""

from datetime import UTC, date, datetime
from logging import getLogger
from typing import Any, cast

import pytest
from anibridge.provider.base import RecordField

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
