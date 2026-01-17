"""Client for the MyAnimeList v2 API."""

import asyncio
import contextlib
import importlib.metadata
from collections.abc import Sequence
from datetime import UTC, date, tzinfo
from logging import getLogger
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from limiter import Limiter

from anibridge_mal_provider.models import (
    Anime,
    AnimePaging,
    MalListStatus,
    MyAnimeListStatus,
    User,
)

__all__ = ["MalClient"]

_LOG = getLogger(__name__)

TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"

mal_limiter = Limiter(rate=1, capacity=1, jitter=False)


class MalClient:
    """Client for the MAL REST API."""

    API_URL = "https://api.myanimelist.net/v2"
    DEFAULT_ANIME_FIELDS = (
        "id",
        "title",
        "main_picture",
        "alternative_titles",
        "start_date",
        "end_date",
        "mean",
        "rank",
        "popularity",
        "num_list_users",
        "num_scoring_users",
        "nsfw",
        "media_type",
        "status",
        "num_episodes",
        "start_season",
        "broadcast",
        "source",
        "average_episode_duration",
        "rating",
        "genres",
        "my_list_status{status,score,num_episodes_watched,is_rewatching,start_date,finish_date,priority,num_times_rewatched,rewatch_value,tags,comments,updated_at}",
    )

    def __init__(
        self,
        *,
        client_id: str,
        refresh_token: str | None = None,
    ) -> None:
        """Construct the client with the required credentials."""
        self.client_id = client_id
        self.access_token: str | None = None
        self._session: aiohttp.ClientSession | None = None

        self.refresh_token = refresh_token

        self.user: User | None = None
        self.user_timezone: tzinfo = UTC
        self.offline_anime_entries: dict[int, Anime] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {
                "Accept": "application/json",
                "User-Agent": "anibridge-mal-provider/"
                + importlib.metadata.version("anibridge-mal-provider"),
                "X-MAL-CLIENT-ID": self.client_id,
            }
            if self.access_token:
                headers["Authorization"] = f"Bearer {self.access_token}"

            self._session = aiohttp.ClientSession(headers=headers)

        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session if it is open."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def initialize(self) -> None:
        """Prime the client by fetching user info and clearing caches."""
        self.offline_anime_entries.clear()
        await self.refresh_access_token()
        self.user = await self.get_user()
        if self.user and self.user.time_zone:
            with contextlib.suppress(Exception):
                self.user_timezone = ZoneInfo(self.user.time_zone)

    async def get_user(self, username: str = "@me") -> User:
        """Fetch user info for the given username (defaulting to self)."""
        response = await self._make_request(
            "GET",
            f"/users/{username}",
            params={"fields": "time_zone"},
        )
        return User(**response)

    async def search_anime(
        self,
        query: str,
        *,
        limit: int = 10,
        nsfw: bool = False,
        fields: Sequence[str] | None = None,
    ) -> list[Anime]:
        """Search anime by title with optional NSFW filtering."""
        effective_fields = fields or self.DEFAULT_ANIME_FIELDS
        params = {
            "q": query,
            "limit": min(max(limit, 1), 100),
            "nsfw": str(nsfw).lower(),
            "fields": ",".join(effective_fields),
        }
        response = await self._make_request("GET", "/anime", params=params)
        paging = AnimePaging(**response)
        results: list[Anime] = []
        for item in paging.data:
            anime = item.node
            if item.list_status is not None:
                anime.my_list_status = item.list_status
            self.offline_anime_entries[anime.id] = anime
            results.append(anime)
        return results

    async def get_anime(
        self,
        anime_id: int,
        *,
        fields: Sequence[str] | None = None,
        force_refresh: bool = False,
    ) -> Anime:
        """Retrieve anime details by id, using cache unless forced."""
        if not force_refresh and anime_id in self.offline_anime_entries:
            return self.offline_anime_entries[anime_id]

        effective_fields = fields or self.DEFAULT_ANIME_FIELDS
        params = {"fields": ",".join(effective_fields)}
        response = await self._make_request("GET", f"/anime/{anime_id}", params=params)
        anime = Anime(**response)
        self.offline_anime_entries[anime.id] = anime
        return anime

    async def get_user_anime_list(
        self,
        *,
        username: str = "@me",
        status: MalListStatus | str | None = None,
        limit: int = 100,
        offset: int = 0,
        nsfw: bool = False,
        sort: str | None = None,
        fields: Sequence[str] | None = None,
    ) -> AnimePaging:
        """Fetch a page of anime list entries for a user."""
        params: dict[str, Any] = {
            "limit": min(max(limit, 1), 100),
            "offset": max(offset, 0),
            "nsfw": str(nsfw).lower(),
            "fields": ",".join(fields or self.DEFAULT_ANIME_FIELDS),
        }
        if status:
            params["status"] = (
                status.value if isinstance(status, MalListStatus) else status
            )
        if sort:
            params["sort"] = sort

        response = await self._make_request(
            "GET",
            f"/users/{username}/animelist",
            params=params,
        )
        paging = AnimePaging(**response)
        for item in paging.data:
            anime = item.node
            if item.list_status is not None:
                anime.my_list_status = item.list_status
            self.offline_anime_entries[anime.id] = anime
        return paging

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
        tags: Sequence[str] | None = None,
        comments: str | None = None,
    ) -> MyAnimeListStatus:
        """Create or update a user's anime list entry."""
        if not self.access_token:
            raise aiohttp.ClientError("Access token is required to update list entries")

        payload: dict[str, str] = {}
        if status is not None:
            payload["status"] = (
                status.value if isinstance(status, MalListStatus) else str(status)
            )
        if score is not None:
            payload["score"] = str(score)
        if progress is not None:
            payload["num_watched_episodes"] = str(progress)
        if is_rewatching is not None:
            payload["is_rewatching"] = str(is_rewatching).lower()
        if start_date is not None:
            payload["start_date"] = start_date.isoformat()
        if finish_date is not None:
            payload["finish_date"] = finish_date.isoformat()
        if priority is not None:
            payload["priority"] = str(priority)
        if num_times_rewatched is not None:
            payload["num_times_rewatched"] = str(num_times_rewatched)
        if rewatch_value is not None:
            payload["rewatch_value"] = str(rewatch_value)
        if tags:
            payload["tags"] = ",".join(tags)
        if comments is not None:
            payload["comments"] = comments

        response = await self._make_request(
            "PATCH",
            f"/anime/{anime_id}/my_list_status",
            data=payload,
        )
        status_payload = response.get("my_list_status", response)
        self.offline_anime_entries.pop(anime_id, None)
        return MyAnimeListStatus(**status_payload)

    async def delete_anime_status(self, anime_id: int) -> None:
        """Remove a user's anime list entry."""
        if not self.access_token:
            raise aiohttp.ClientError("Access token is required to delete list entries")

        await self._make_request("DELETE", f"/anime/{anime_id}/my_list_status")
        self.offline_anime_entries.pop(anime_id, None)

    async def clear_cache(self) -> None:
        """Clear any cached anime payloads."""
        self.offline_anime_entries.clear()

    async def refresh_access_token(self) -> None:
        """Refresh the access token using the stored refresh credentials."""
        if not self.refresh_token:
            raise ValueError("Refresh token is not configured")

        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": self.refresh_token,
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "anibridge-mal-provider/"
            + importlib.metadata.version("anibridge-mal-provider"),
        }

        async with (
            aiohttp.ClientSession(headers=headers) as session,
            session.post(TOKEN_URL, data=payload) as response,
        ):
            response.raise_for_status()
            data = await response.json()

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", self.refresh_token)
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    @mal_limiter()
    async def _make_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        data: Any = None,
        retry_count: int = 0,
        refresh_attempted: bool = False,
    ) -> dict[str, Any]:
        if retry_count >= 3:
            raise aiohttp.ClientError("Failed to make request after 3 tries")

        session = await self._get_session()
        url = f"{self.API_URL.rstrip('/')}/{path.lstrip('/')}"

        try:
            async with session.request(
                method,
                url,
                params=params,
                json=json,
                data=data,
            ) as response:
                if (
                    response.status == 401
                    and not refresh_attempted
                    and self.refresh_token
                ):
                    await self.refresh_access_token()
                    return await self._make_request(
                        method,
                        path,
                        params=params,
                        json=json,
                        data=data,
                        retry_count=retry_count,
                        refresh_attempted=True,
                    )
                if response.status in (429, 502):
                    retry_after = int(response.headers.get("Retry-After", "1"))
                    await asyncio.sleep(max(retry_after, 1))
                    return await self._make_request(
                        method,
                        path,
                        params=params,
                        json=json,
                        data=data,
                        retry_count=retry_count + 1,
                    )

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError:
                    response_text = await response.text()
                    _LOG.error(
                        "Failed MAL request %s %s (%s): %s",
                        method,
                        url,
                        response.status,
                        response_text,
                    )
                    raise

                if response.status == 204:
                    return {}
                return await response.json()

        except (TimeoutError, aiohttp.ClientError):
            await asyncio.sleep(1)
            return await self._make_request(
                method,
                path,
                params=params,
                json=json,
                data=data,
                retry_count=retry_count + 1,
            )

    @staticmethod
    def parse_date(value: Any) -> date | None:
        """Parse a date value from MAL API."""
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        if not isinstance(value, str):
            return None
        with contextlib.suppress(ValueError):
            return date.fromisoformat(str(value))

        parts = value.split("-")
        try:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1
            return date(year, month, day)
        except (ValueError, IndexError):
            return None
