"""MyAnimeList list provider for AniBridge."""

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast

from anibridge.list import (
    ListEntry,
    ListMedia,
    ListMediaType,
    ListProvider,
    ListStatus,
    ListTarget,
    ListUser,
    list_provider,
)

from anibridge_mal_provider.client import MalClient
from anibridge_mal_provider.models import (
    Anime,
    MalListStatus,
    MyAnimeListStatus,
)

__all__ = ["MalListProvider"]

_DEFAULT_CLIENT_ID = "b11a4e1ead0db8142268906b4bb676a4"


def _mal_status_to_list(status: MalListStatus | str | None) -> ListStatus | None:
    if status is None:
        return None
    mapping = {
        MalListStatus.WATCHING: ListStatus.CURRENT,
        MalListStatus.COMPLETED: ListStatus.COMPLETED,
        MalListStatus.ON_HOLD: ListStatus.PAUSED,
        MalListStatus.DROPPED: ListStatus.DROPPED,
        MalListStatus.PLAN_TO_WATCH: ListStatus.PLANNING,
    }
    try:
        enum_status = MalListStatus(status)
    except ValueError:
        return None
    return mapping.get(enum_status)


def _list_status_to_mal(status: ListStatus | None) -> tuple[MalListStatus | None, bool]:
    if status is None:
        return None, False
    mal_status: MalListStatus | None
    is_rewatching = status is ListStatus.REPEATING

    if status is ListStatus.CURRENT:
        mal_status = MalListStatus.WATCHING
    elif status is ListStatus.COMPLETED:
        mal_status = MalListStatus.COMPLETED
    elif status is ListStatus.DROPPED:
        mal_status = MalListStatus.DROPPED
    elif status is ListStatus.PAUSED:
        mal_status = MalListStatus.ON_HOLD
    elif status is ListStatus.PLANNING:
        mal_status = MalListStatus.PLAN_TO_WATCH
    elif status is ListStatus.REPEATING:
        mal_status = MalListStatus.WATCHING
    else:
        mal_status = None

    return mal_status, is_rewatching


class MalListMedia(ListMedia["MalListProvider"]):
    """AniBridge media wrapper for MAL anime resources."""

    def __init__(self, provider: MalListProvider, anime: Anime) -> None:
        self._provider = provider
        self._anime = anime
        self._key = str(anime.id)
        self._title = anime.title

    @property
    def external_url(self) -> str | None:
        return f"https://myanimelist.net/anime/{self._key}"

    @property
    def labels(self) -> Sequence[str]:
        labels: list[str] = []
        if self._anime.start_season and self._anime.start_season.year:
            season = self._anime.start_season.season
            if season:
                labels.append(f"{season.title()} {self._anime.start_season.year}")
            else:
                labels.append(str(self._anime.start_season.year))
        if self._anime.media_type:
            labels.append(self._anime.media_type.replace("_", " ").title())
        if self._anime.status:
            labels.append(self._anime.status.replace("_", " ").title())
        return labels

    @property
    def media_type(self) -> ListMediaType:
        if self._anime.media_type and self._anime.media_type.lower() == "movie":
            return ListMediaType.MOVIE
        return ListMediaType.TV

    @property
    def total_units(self) -> int | None:
        if self._anime.num_episodes:
            return self._anime.num_episodes
        if self.media_type is ListMediaType.MOVIE:
            return 1
        return None

    @property
    def poster_image(self) -> str | None:
        if self._anime.main_picture is None:
            return None
        return self._anime.main_picture.large or self._anime.main_picture.medium

    def provider(self) -> MalListProvider:
        return self._provider


class MalListEntry(ListEntry["MalListProvider"]):
    """AniBridge list entry backed by MAL list status."""

    def __init__(self, provider: MalListProvider, anime: Anime) -> None:
        self._provider = provider
        self._anime = anime
        self._media = MalListMedia(provider, anime)
        self._status = anime.my_list_status or MyAnimeListStatus()

        self._key = str(anime.id)
        self._title = anime.title

    @property
    def status(self) -> ListStatus | None:
        return _mal_status_to_list(self._status.status)

    @status.setter
    def status(self, value: ListStatus | None) -> None:
        mal_status, is_rewatching = _list_status_to_mal(value)
        self._status.status = mal_status
        if value is not None:
            self._status.is_rewatching = is_rewatching

    @property
    def progress(self) -> int:
        return self._status.num_episodes_watched or 0

    @progress.setter
    def progress(self, value: int | None) -> None:
        if value is None:
            self._status.num_episodes_watched = None
            return
        if value < 0:
            raise ValueError("Progress cannot be negative.")
        self._status.num_episodes_watched = value

    @property
    def repeats(self) -> int:
        return self._status.num_times_rewatched or 0

    @repeats.setter
    def repeats(self, value: int | None) -> None:
        if value is None:
            self._status.num_times_rewatched = None
            return
        if value < 0:
            raise ValueError("Repeat count cannot be negative.")
        self._status.num_times_rewatched = value

    @property
    def review(self) -> str | None:
        return self._status.comments

    @review.setter
    def review(self, value: str | None) -> None:
        self._status.comments = value

    @property
    def user_rating(self) -> int | None:
        if self._status.score is None:
            return None
        return int(self._status.score * 10)

    @user_rating.setter
    def user_rating(self, value: int | None) -> None:
        if value is None:
            self._status.score = None
            return
        if value < 0 or value > 100:
            raise ValueError("Ratings must be between 0 and 100.")
        self._status.score = round(value / 10)

    @property
    def started_at(self) -> datetime | None:
        if self._status.start_date is None:
            return None
        return datetime.combine(
            self._status.start_date,
            datetime.min.time(),
            tzinfo=self._provider._client.user_timezone,
        )

    @started_at.setter
    def started_at(self, value: datetime | None) -> None:
        if value is None:
            self._status.start_date = None
            return
        self._status.start_date = value.astimezone(
            self._provider._client.user_timezone
        ).date()

    @property
    def finished_at(self) -> datetime | None:
        if self._status.finish_date is None:
            return None
        return datetime.combine(
            self._status.finish_date,
            datetime.min.time(),
            tzinfo=self._provider._client.user_timezone,
        )

    @finished_at.setter
    def finished_at(self, value: datetime | None) -> None:
        if value is None:
            self._status.finish_date = None
            return
        self._status.finish_date = value.astimezone(
            self._provider._client.user_timezone
        ).date()

    @property
    def total_units(self) -> int | None:
        return self._media.total_units

    def media(self) -> MalListMedia:
        return self._media

    def provider(self) -> MalListProvider:
        return self._provider


@list_provider
class MalListProvider(ListProvider):
    """List provider backed by the MyAnimeList v2 API."""

    NAMESPACE = "mal"
    MAPPING_PROVIDERS = frozenset({"mal"})

    def __init__(self, *, config: dict | None = None) -> None:
        """Create the MAL list provider with required credentials."""
        self.config = config or {}
        client_id = self.config.get("client_id", _DEFAULT_CLIENT_ID)
        refresh_token = self.config.get("token")
        if not client_id:
            raise ValueError("MAL client_id must be provided in the configuration")
        if not refresh_token:
            raise ValueError("MAL refresh_token must be provided in the configuration")

        self._client = MalClient(client_id=client_id, refresh_token=refresh_token)
        self._user: ListUser | None = None

    async def initialize(self) -> None:
        """Fetch MAL user info and prepare caches."""
        await self._client.initialize()
        if self._client.user is not None:
            self._user = ListUser(
                key=str(self._client.user.id),
                title=self._client.user.name,
            )

    async def backup_list(self) -> str:
        """Return a JSON backup of the user's MAL anime list."""
        entries: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await self._client.get_user_anime_list(offset=offset, limit=100)
            for item in page.data:
                status = item.list_status or item.node.my_list_status
                if status is None:
                    continue
                entries.append(
                    {
                        "id": item.node.id,
                        "status": status.status,
                        "score": status.score,
                        "num_watched_episodes": status.num_episodes_watched,
                        "is_rewatching": status.is_rewatching,
                        "start_date": (
                            status.start_date.isoformat() if status.start_date else None
                        ),
                        "finish_date": (
                            status.finish_date.isoformat()
                            if status.finish_date
                            else None
                        ),
                        "priority": status.priority,
                        "num_times_rewatched": status.num_times_rewatched,
                        "rewatch_value": status.rewatch_value,
                        "tags": status.tags,
                        "comments": status.comments,
                    }
                )
            if page.paging is None or page.paging.next is None:
                break
            offset += 100
        return json.dumps(entries, separators=(",", ":"))

    async def delete_entry(self, key: str) -> None:
        """Delete a list entry by MAL anime id."""
        await self._client.delete_anime_status(int(key))

    async def get_entry(self, key: str) -> MalListEntry | None:
        """Fetch a single entry from cache or by building it on demand."""
        anime = await self._client.get_anime(int(key))
        if anime.my_list_status is None:
            anime.my_list_status = MyAnimeListStatus()
        return MalListEntry(self, anime)

    async def derive_keys(
        self, descriptors: Sequence[tuple[str, str, str | None]]
    ) -> set[str]:
        """Resolve mapping descriptors into MAL media keys."""
        return {
            entry_id
            for provider, entry_id, _ in descriptors
            if provider == self.NAMESPACE and entry_id
        }

    async def resolve_mapping_descriptors(
        self, descriptors: Sequence[tuple[str, str, str | None]]
    ) -> Sequence[ListTarget]:
        """Resolve mapping descriptors into MAL media keys."""
        return [
            ListTarget(descriptor=(provider, entry_id, scope), media_key=entry_id)
            for provider, entry_id, scope in descriptors
            if provider in self.MAPPING_PROVIDERS and entry_id
        ]

    async def restore_list(self, backup: str) -> None:
        """Restore list entries from a JSON backup string."""
        data = json.loads(backup)
        for item in data:
            await self._client.update_anime_status(
                anime_id=int(item["id"]),
                status=item.get("status"),
                score=item.get("score"),
                progress=item.get("num_watched_episodes"),
                is_rewatching=item.get("is_rewatching"),
                start_date=(MalClient.parse_date(item.get("start_date"))),
                finish_date=(MalClient.parse_date(item.get("finish_date"))),
                priority=item.get("priority"),
                num_times_rewatched=item.get("num_times_rewatched"),
                rewatch_value=item.get("rewatch_value"),
                tags=item.get("tags"),
                comments=item.get("comments"),
            )

    async def search(self, query: str) -> Sequence[MalListEntry]:
        """Search MAL and return entries with minimal metadata and status."""
        results = await self._client.search_anime(query, limit=10, nsfw=False)
        return tuple(MalListEntry(self, anime) for anime in results)

    async def update_entry(self, key: str, entry: ListEntry) -> None:
        """Update a MAL list entry and refresh local cache."""
        mal_entry = cast(MalListEntry, entry)
        status_value, is_rewatching = _list_status_to_mal(mal_entry.status)
        response = await self._client.update_anime_status(
            anime_id=int(key),
            status=status_value,
            score=mal_entry._status.score,
            progress=mal_entry.progress,
            is_rewatching=is_rewatching or bool(mal_entry._status.is_rewatching),
            start_date=mal_entry._status.start_date,
            finish_date=mal_entry._status.finish_date,
            num_times_rewatched=mal_entry._status.num_times_rewatched,
            comments=mal_entry._status.comments,
            tags=mal_entry._status.tags,
        )
        mal_entry._status = response
        mal_entry._anime.my_list_status = response

    async def clear_cache(self) -> None:
        """Clear cached user/list data."""
        await self._client.clear_cache()

    async def close(self) -> None:
        """Close the underlying MAL client session."""
        await self._client.close()

    async def update_entries_batch(
        self, entries: Sequence[ListEntry]
    ) -> Sequence[MalListEntry | None]:
        """Batch update list entries sequentially."""
        updated: list[MalListEntry | None] = []
        for entry in entries:
            await self.update_entry(entry.media().key, entry)
            updated.append(cast(MalListEntry, entry))
        return updated

    async def get_entries_batch(
        self, keys: Sequence[str]
    ) -> Sequence[MalListEntry | None]:
        """Batch fetch list entries, returning None when missing."""
        results: list[MalListEntry | None] = []
        for key in keys:
            results.append(await self.get_entry(key))
        return results

    def user(self) -> ListUser | None:
        """Return cached MAL user info if initialized."""
        return self._user
