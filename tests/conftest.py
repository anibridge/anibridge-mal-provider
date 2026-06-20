"""Pytest fixtures shared across the provider test-suite."""

from collections.abc import Generator
from datetime import UTC, date

import pytest
from anibridge.utils.limiter import Limiter

from anibridge.providers.mal.client import MalClient
from anibridge.providers.mal.models import (
    Anime,
    AnimePaging,
    AnimePagingData,
    MalListStatus,
    MyAnimeListStatus,
    User,
)


class _FakeMalClient:
    """Lightweight MAL client stub used by tests."""

    def __init__(self) -> None:
        self.user = User(id=1, name="Tester", time_zone="UTC")
        self.user_timezone = UTC
        self.offline_anime_entries: dict[int, Anime] = {}
        self.update_calls: list[dict] = []
        self.deleted_ids: list[int] = []

    async def initialize(self) -> None:
        """Initialize stub client; nothing to fetch."""

    async def close(self) -> None:
        """Close stub client; nothing to release."""

    async def clear_cache(self) -> None:
        """Clear cached entries and recorded calls."""
        self.offline_anime_entries.clear()
        self.update_calls.clear()
        self.deleted_ids.clear()

    async def get_user(self, username: str = "@me") -> User:
        """Return the configured stub user."""
        return self.user

    async def get_anime(self, anime_id: int, *, force_refresh: bool = False):
        """Return an anime from the in-memory cache."""
        return self.offline_anime_entries[anime_id]

    async def search_anime(self, query: str, *, limit: int = 10, nsfw: bool = False):
        """Return cached anime entries up to the requested limit."""
        return list(self.offline_anime_entries.values())[:limit]

    async def get_user_anime_list(
        self,
        *,
        username: str = "@me",
        status: MalListStatus | str | None = None,
        limit: int = 1000,
        offset: int = 0,
        nsfw: bool = False,
        sort: str | None = None,
        fields=None,
    ) -> AnimePaging:
        """Return a paginated slice of cached anime entries."""
        items = list(self.offline_anime_entries.values())[offset : offset + limit]
        data = [
            AnimePagingData(node=anime, list_status=anime.my_list_status)
            for anime in items
        ]
        return AnimePaging(data=data, paging=None)

    async def update_anime_status(
        self,
        anime_id: int,
        *,
        status: MalListStatus | str | None = None,
        score: int | None = None,
        progress: int | None = None,
        is_rewatching: bool | None = None,
        start_date: date | None = None,
        finish_date: date | None = None,
        priority: int | None = None,
        num_times_rewatched: int | None = None,
        rewatch_value: int | None = None,
        tags=None,
        comments: str | None = None,
    ) -> MyAnimeListStatus:
        """Record an update and return the resulting status."""
        call = {
            "anime_id": anime_id,
            "status": status,
            "score": score,
            "progress": progress,
            "is_rewatching": is_rewatching,
            "start_date": start_date,
            "finish_date": finish_date,
            "num_times_rewatched": num_times_rewatched,
            "tags": list(tags or []),
            "comments": comments,
        }
        self.update_calls.append(call)
        normalized_status = (
            None
            if status is None
            else status
            if isinstance(status, MalListStatus)
            else MalListStatus(status)
        )
        status_model = MyAnimeListStatus(
            status=normalized_status,
            score=score,
            num_episodes_watched=progress,
            is_rewatching=is_rewatching,
            start_date=start_date,
            finish_date=finish_date,
            num_times_rewatched=num_times_rewatched,
            tags=list(tags or []),
            comments=comments,
        )
        anime = self.offline_anime_entries.setdefault(
            anime_id, Anime(id=anime_id, title=f"Anime {anime_id}")
        )
        anime.my_list_status = status_model
        return status_model

    async def delete_anime_status(self, anime_id: int) -> None:
        """Record deletion and drop cached entry."""
        self.deleted_ids.append(anime_id)
        self.offline_anime_entries.pop(anime_id, None)


@pytest.fixture()
def fake_client() -> _FakeMalClient:
    """Return an isolated fake MAL client instance."""
    return _FakeMalClient()


@pytest.fixture(autouse=True)
def disable_rate_limiter(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    """Disable limiter behavior and unwrap decorated methods for fast tests."""
    previous = Limiter.DISABLED
    Limiter.DISABLED = True
    wrapped = getattr(MalClient._make_request, "__wrapped__", None)
    if wrapped is not None:
        monkeypatch.setattr(MalClient, "_make_request", wrapped)
    search_wrapped = getattr(MalClient._search_anime, "__wrapped__", None)
    if search_wrapped is not None:
        monkeypatch.setattr(MalClient, "_search_anime", search_wrapped)
    yield
    Limiter.DISABLED = previous
