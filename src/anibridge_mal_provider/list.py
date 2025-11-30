"""MyAnimeList implementation of the AniBridge list provider interface."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from aiohttp import FormData
from anibridge.list import (
    ListEntry,
    ListMedia,
    ListMediaType,
    ListProvider,
    ListStatus,
    ListUser,
    list_provider,
)

from anibridge_mal_provider.client import MalClient
from anibridge_mal_provider.models import (
    MalListEntryStatus,
    MalListStatus,
    MalMedia,
    MalMediaFormat,
)

__all__ = ["MalListEntry", "MalListMedia", "MalListProvider"]

MAL_STATUS_TO_LIST: Final[dict[MalListEntryStatus, ListStatus]] = {
    MalListEntryStatus.WATCHING: ListStatus.CURRENT,
    MalListEntryStatus.COMPLETED: ListStatus.COMPLETED,
    MalListEntryStatus.ON_HOLD: ListStatus.PAUSED,
    MalListEntryStatus.DROPPED: ListStatus.DROPPED,
    MalListEntryStatus.PLAN_TO_WATCH: ListStatus.PLANNING,
}


@dataclass(slots=True)
class MalEntryPayload:
    """Bundle the MAL media payload with its mutable list status."""

    media: MalMedia
    status: MalListStatus

    @classmethod
    def from_media(cls, media: MalMedia) -> MalEntryPayload:
        """Ensure there is a status object attached to the MAL media payload."""
        status = media.my_list_status
        if status is None:
            status = MalListStatus()
            media.my_list_status = status
        return cls(media=media, status=status)


class MalListMedia(ListMedia):
    """List media wrapper for MAL anime entries."""

    def __init__(self, provider: MalListProvider, media: MalMedia) -> None:
        """Initialize the MAL list media adapter."""
        self._provider = provider
        self._media = media
        self.key = str(media.id)
        self.title = media.title

    @property
    def media_type(self) -> ListMediaType:
        """Return the media type of the MAL entry."""
        if self._media.media_type == MalMediaFormat.MOVIE:
            return ListMediaType.MOVIE
        return ListMediaType.TV

    @property
    def total_units(self) -> int | None:
        """Return the total units (episodes) of the MAL entry."""
        if self._media.num_episodes:
            return self._media.num_episodes
        if self.media_type == ListMediaType.MOVIE:
            return 1
        return None

    @property
    def poster_image(self) -> str | None:
        """Return the poster image URL of the MAL entry."""
        picture = self._media.main_picture
        if picture is None:
            return None
        return picture.large or picture.medium

    def provider(self) -> MalListProvider:
        """Return the parent provider of the MAL list media."""
        return self._provider


class MalListEntry(ListEntry):
    """Concrete list entry that adapts MAL payloads to AniBridge."""

    def __init__(self, provider: MalListProvider, payload: MalEntryPayload) -> None:
        """Bind the MAL payload bundle to the AniBridge list entry."""
        self._provider = provider
        self._payload = payload
        self._media = MalListMedia(provider, payload.media)
        self.key = str(payload.media.id)
        self.title = payload.media.title

    @property
    def status(self) -> ListStatus | None:
        """Return the list status of the MAL entry."""
        mal_status = self._payload.status
        if mal_status.is_rewatching:
            return ListStatus.REPEATING
        if mal_status.status is None:
            return None
        return MAL_STATUS_TO_LIST.get(mal_status.status)

    @status.setter
    def status(self, value: ListStatus | None) -> None:
        """Set the list status of the MAL entry."""
        mal_status = self._payload.status
        mal_status.is_rewatching = value == ListStatus.REPEATING
        mal_status.status = _to_mal_status(value)

    @property
    def progress(self) -> int:
        """Return the progress (watched episodes) of the MAL entry."""
        return self._payload.status.num_episodes_watched

    @progress.setter
    def progress(self, value: int | None) -> None:
        """Set the progress (watched episodes) of the MAL entry."""
        if value is None:
            self._payload.status.num_episodes_watched = 0
            return
        if value < 0:
            raise ValueError("Progress cannot be negative")
        self._payload.status.num_episodes_watched = value

    @property
    def repeats(self) -> int:
        """Return the repeat count of the MAL entry."""
        return self._payload.status.num_times_rewatched

    @repeats.setter
    def repeats(self, value: int | None) -> None:
        """Set the repeat count of the MAL entry."""
        if value is None:
            self._payload.status.num_times_rewatched = 0
            return
        if value < 0:
            raise ValueError("Repeat count cannot be negative")
        self._payload.status.num_times_rewatched = value

    @property
    def review(self) -> str | None:
        """Return the review notes of the MAL entry."""
        comments = self._payload.status.comments
        return comments or None

    @review.setter
    def review(self, value: str | None) -> None:
        """Set the review notes of the MAL entry."""
        self._payload.status.comments = value or ""

    @property
    def user_rating(self) -> int | None:
        """Return the user rating of the MAL entry."""
        score = self._payload.status.score
        if not score:
            return None
        return int(score * 10)

    @user_rating.setter
    def user_rating(self, value: int | None) -> None:
        """Set the user rating of the MAL entry."""
        if value is None:
            self._payload.status.score = 0
            return
        if value < 0 or value > 100:
            raise ValueError("Ratings must be between 0 and 100")
        self._payload.status.score = round(value / 10)

    @property
    def started_at(self) -> datetime | None:
        """Return the start date of the MAL entry."""
        if self._payload.status.start_date is None:
            return None
        return datetime.combine(self._payload.status.start_date, datetime.min.time())

    @started_at.setter
    def started_at(self, value: datetime | None) -> None:
        """Set the start date of the MAL entry."""
        self._payload.status.start_date = value.date() if value else None

    @property
    def finished_at(self) -> datetime | None:
        """Return the finish date of the MAL entry."""
        if self._payload.status.finish_date is None:
            return None
        return datetime.combine(self._payload.status.finish_date, datetime.min.time())

    @finished_at.setter
    def finished_at(self, value: datetime | None) -> None:
        """Set the finish date of the MAL entry."""
        self._payload.status.finish_date = value.date() if value else None

    @property
    def total_units(self) -> int | None:
        """Return the total units of the MAL entry."""
        return self._media.total_units

    def media(self) -> MalListMedia:
        """Return the media associated with the MAL list entry."""
        return self._media

    def provider(self) -> MalListProvider:
        """Return the parent provider of the MAL list entry."""
        return self._provider

    def mal_media(self) -> MalMedia:
        """Return the underlying MAL media payload."""
        return self._payload.media

    def mal_status(self) -> MalListStatus:
        """Return the raw MAL list status payload."""
        return self._payload.status


@list_provider
class MalListProvider(ListProvider):
    """AniBridge list provider backed by the MyAnimeList REST API."""

    NAMESPACE = "mal"

    def __init__(self, *, config: dict | None = None) -> None:
        """Configure the provider with MAL OAuth settings."""
        self.config = config or {}
        client_id = self.config.get("client_id")
        access_token = self.config.get("access_token")
        refresh_token = self.config.get("refresh_token")
        client_secret = self.config.get("client_secret")

        if not client_id:
            raise ValueError("MAL client_id must be supplied via configuration")
        if not access_token:
            raise ValueError("MAL access_token must be supplied via configuration")

        self._client = MalClient(
            client_id=client_id,
            access_token=access_token,
            refresh_token=refresh_token,
            client_secret=client_secret,
        )
        self._user: ListUser | None = None

    async def initialize(self) -> None:
        """Prime the client and cache the authenticated MAL user."""
        await self._client.initialize()
        if self._client.user:
            self._user = ListUser(
                key=str(self._client.user.id), title=self._client.user.name
            )

    async def backup_list(self) -> str:
        """Delegate list backup creation to the MAL client."""
        return await self._client.backup_mal()

    async def restore_list(self, backup: str) -> None:
        """Restore previously backed up entries via the MAL client."""
        await self._client.restore_mal(backup)

    async def delete_entry(self, key: str) -> None:
        """Delete the MAL list entry associated with the supplied key."""
        await self._client.delete_anime_entry(int(key))

    async def get_entry(self, key: str) -> MalListEntry | None:
        """Fetch a MAL entry if one exists for the provided key."""
        media = await self._client.get_anime(int(key))
        if media.my_list_status is None:
            return None
        return self._wrap_media(media)

    async def build_entry(self, key: str) -> MalListEntry:
        """Create a writable MAL entry for the supplied media key."""
        media = await self._client.get_anime(int(key))
        return self._wrap_media(media)

    async def search(self, query: str) -> Sequence[MalListEntry]:
        """Search MAL for anime and adapt the results to AniBridge entries."""
        results: list[MalListEntry] = []
        async for media in self._client.search_anime(query, limit=10):
            results.append(self._wrap_media(media))
        return results

    async def update_entry(self, key: str, entry: ListEntry) -> None:
        """Persist entry changes back to MAL for the supplied key."""
        if not isinstance(entry, MalListEntry):
            raise TypeError(
                "MalListProvider can only operate on MalListEntry instances"
            )
        if entry.key != key:
            raise ValueError("Entry key does not match the provided key")
        payload = self._build_media_payload(entry)
        await self._client.update_anime_entry(int(entry.key), payload)

    async def clear_cache(self) -> None:
        """Drop any cached MAL media payloads."""
        self._client.offline_entries.clear()

    async def close(self) -> None:
        """Close the underlying MAL client session."""
        await self._client.close()

    def user(self) -> ListUser | None:
        """Return the cached MAL user, if available."""
        return self._user

    def _wrap_media(self, media: MalMedia) -> MalListEntry:
        """Convert a MAL media payload into a managed list entry."""
        payload = MalEntryPayload.from_media(media)
        return MalListEntry(self, payload)

    def _build_media_payload(self, entry: MalListEntry) -> FormData:
        form_data = FormData()
        status_model = entry.mal_status()

        resolved_status = _to_mal_status(entry.status)
        if resolved_status is not None:
            form_data.add_field("status", resolved_status.value)

        form_data.add_field(
            "is_rewatching", "true" if status_model.is_rewatching else "false"
        )
        form_data.add_field("score", str(status_model.score))
        form_data.add_field(
            "num_watched_episodes", str(status_model.num_episodes_watched)
        )
        form_data.add_field(
            "num_times_rewatched", str(status_model.num_times_rewatched)
        )
        form_data.add_field("comments", status_model.comments or "")

        return form_data


def _to_mal_status(value: ListStatus | None) -> MalListEntryStatus | None:
    """Convert a ListStatus to a MalListEntryStatus, or return None."""
    if value is None:
        return None
    if value == ListStatus.REPEATING:
        return MalListEntryStatus.WATCHING
    if value == ListStatus.CURRENT:
        return MalListEntryStatus.WATCHING
    if value == ListStatus.COMPLETED:
        return MalListEntryStatus.COMPLETED
    if value == ListStatus.PAUSED:
        return MalListEntryStatus.ON_HOLD
    if value == ListStatus.DROPPED:
        return MalListEntryStatus.DROPPED
    if value == ListStatus.PLANNING:
        return MalListEntryStatus.PLAN_TO_WATCH
    return None
