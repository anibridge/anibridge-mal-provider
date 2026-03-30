"""Client for the MyAnimeList v2 API."""

import asyncio
import contextlib
import importlib.metadata
from collections.abc import Sequence
from datetime import UTC, date, tzinfo
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

import aiohttp
from anibridge.utils.cache import TTLDict, ttl_cache
from anibridge.utils.limiter import Limiter
from anibridge.utils.types import ProviderLogger

from anibridge.providers.list.mal.models import (
    Anime,
    AnimePaging,
    MalListStatus,
    MyAnimeListStatus,
    User,
)

__all__ = ["MalClient"]

TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"

global_mal_limiter = Limiter(rate=60 / 60, capacity=1)


class MalClient:
    """Client for the MAL REST API."""

    API_URL: ClassVar[str] = "https://api.myanimelist.net/v2"

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
        "my_list_status{status,score,num_episodes_watched,is_rewatching,start_date,"
        "finish_date,priority,num_times_rewatched,rewatch_value,tags,comments,updated_at}",
    )
    DEFAULT_ANIME_FIELDS_CSV = ",".join(DEFAULT_ANIME_FIELDS)

    def __init__(
        self,
        *,
        logger: ProviderLogger,
        client_id: str,
        refresh_token: str | None = None,
        rate_limit: int | None = None,
    ) -> None:
        """Construct the client with the required credentials."""
        self.log = logger
        self.client_id = client_id
        self.access_token: str | None = None
        self._session: aiohttp.ClientSession | None = None
        self.refresh_token = refresh_token
        self.rate_limit = rate_limit

        if self.rate_limit is None:
            self.log.debug(
                "Using shared global MAL rate limiter with %s requests per minute",
                global_mal_limiter.rate * 60,
            )
            self._request_limiter = global_mal_limiter
        else:
            self.log.debug(
                "Using local MAL rate limiter with %s requests per minute",
                self.rate_limit,
            )
            self._request_limiter = Limiter(rate=self.rate_limit / 60, capacity=1)

        self.user: User | None = None
        self.user_timezone: tzinfo = UTC

        self._bg_task: asyncio.Task[AnimePaging] | None = None
        self._cache_epoch = 0
        self._list_cache: dict[int, Anime] = {}
        self._media_cache: TTLDict[int, Anime] = TTLDict(ttl=43200)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
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
        if (task := self._bg_task) and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._session and not self._session.closed:
            await self._session.close()

    def clear_cache(self) -> None:
        """Clear in-memory caches for user list and general media lookups."""
        self._list_cache.clear()
        self._media_cache.clear()
        self._invalidate_cached_views()

    def _invalidate_cached_views(self) -> None:
        """Invalidate derived cached views after list-state changes."""
        self._cache_epoch += 1
        if (task := self._bg_task) and not task.done():
            task.cancel()
        self._bg_task = None
        with contextlib.suppress(AttributeError):
            self._fetch_list_collection.cache_clear()
        with contextlib.suppress(AttributeError):
            self._search_anime.cache_clear()

    async def initialize(self) -> None:
        """Prime the client by fetching user info and clearing caches."""
        self.clear_cache()
        await self.refresh_access_token()
        self.user = await self.get_user()

        if self.user and self.user.time_zone:
            with contextlib.suppress(Exception):
                self.user_timezone = ZoneInfo(self.user.time_zone)

        await self._fetch_list_collection()

    def _cached(self, anime_id: int) -> Anime | None:
        """Return anime from user-list cache or general TTL cache."""
        hit = self._list_cache.get(anime_id) or self._media_cache.get(anime_id)
        if hit:
            self.log.debug(f"Cache hit $${{mal_id: {anime_id}}}$$")
        return hit

    def _remember(self, anime: Anime) -> None:
        """Store anime in shared caches."""
        # Keep the long-lived TTL cache free of user-specific list state.
        self._media_cache[anime.id] = anime.model_copy(update={"my_list_status": None})
        if anime.my_list_status is None:
            self._list_cache.pop(anime.id, None)
        else:
            self._list_cache[anime.id] = anime

    def _schedule_list_refresh(self) -> None:
        """Schedule a background refresh when the user-list cache is stale.

        The @ttl_cache on _fetch_list_collection returns instantly from cache
        when fresh, so this is a no-op in the common case.
        """
        if (task := self._bg_task) and not task.done():
            return

        def _on_done(t: asyncio.Task[AnimePaging]) -> None:
            if not t.cancelled() and (exc := t.exception()):
                self.log.warning("User-list cache refresh failed", exc_info=exc)

        self._bg_task = task = asyncio.create_task(self._fetch_list_collection())
        task.add_done_callback(_on_done)

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
        nsfw: bool = True,
        fields: Sequence[str] | None = None,
    ) -> list[Anime]:
        """Search anime by title."""
        normalized_fields = tuple(fields) if fields is not None else None
        return await self._search_anime(
            query,
            limit=min(max(limit, 1), 100),
            nsfw=nsfw,
            fields=normalized_fields,
        )

    @ttl_cache(ttl=300)
    async def _search_anime(
        self,
        query: str,
        *,
        limit: int = 10,
        nsfw: bool = True,
        fields: tuple[str, ...] | None = None,
    ) -> list[Anime]:
        """Cached helper for anime title searches."""
        effective_fields = fields or self.DEFAULT_ANIME_FIELDS
        params = {
            "q": query,
            "limit": limit,
            "nsfw": str(nsfw).lower(),
            "fields": (
                self.DEFAULT_ANIME_FIELDS_CSV
                if effective_fields is self.DEFAULT_ANIME_FIELDS
                else ",".join(effective_fields)
            ),
        }
        response = await self._make_request("GET", "/anime", params=params)
        paging = AnimePaging(**response)
        results: list[Anime] = []
        for item in paging.data:
            anime = item.node
            if item.list_status is not None:
                anime.my_list_status = item.list_status
            self._remember(anime)
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
        self._schedule_list_refresh()
        if not force_refresh:
            cached = self._cached(anime_id)
            if cached is not None:
                return cached
        return await self._fetch_anime(anime_id, fields=fields)

    async def _fetch_anime(
        self,
        anime_id: int,
        *,
        fields: Sequence[str] | None = None,
    ) -> Anime:
        """Fetch an anime from the MAL API and populate caches."""
        params = {
            "fields": (
                self.DEFAULT_ANIME_FIELDS_CSV if fields is None else ",".join(fields)
            )
        }
        self.log.debug(f"Pulling MAL data from API $${{mal_id: {anime_id}}}$$")
        response = await self._make_request("GET", f"/anime/{anime_id}", params=params)
        anime = Anime(**response)
        self._remember(anime)
        return anime

    async def get_user_anime_list(
        self,
        *,
        username: str = "@me",
        status: MalListStatus | str | None = None,
        limit: int = 1000,
        offset: int = 0,
        nsfw: bool = True,
        sort: str | None = None,
        fields: Sequence[str] | None = None,
    ) -> AnimePaging:
        """Fetch a page of anime list entries for a user."""
        normalized_fields = tuple(fields) if fields is not None else None
        page = await self._get_user_anime_list_page(
            username=username,
            status=status,
            limit=limit,
            offset=offset,
            nsfw=nsfw,
            sort=sort,
            fields=normalized_fields,
        )
        for item in page.data:
            anime = item.node
            if item.list_status is not None:
                anime.my_list_status = item.list_status
            self._remember(anime)
        return page

    @ttl_cache(ttl=3600)
    async def _fetch_list_collection(self) -> AnimePaging:
        """Fetch all user list pages and atomically refresh list cache."""
        if not self.user:
            raise aiohttp.ClientError("User information is required for list refresh")

        self.log.debug("Refreshing user anime list cache from MAL API")

        refresh_epoch = self._cache_epoch
        refreshed_list_cache: dict[int, Anime] = {}

        data = AnimePaging(data=[], paging=None)
        offset = 0
        while True:
            page = await self._get_user_anime_list_page(offset=offset, limit=1000)
            if refresh_epoch != self._cache_epoch:
                return data

            data.data.extend(page.data)
            for item in page.data:
                anime = item.node
                if item.list_status is not None:
                    anime.my_list_status = item.list_status
                if anime.my_list_status is not None:
                    refreshed_list_cache[anime.id] = anime
                self._remember(anime)

            if page.paging is None or page.paging.next is None:
                break
            offset += 1000

        if refresh_epoch != self._cache_epoch:
            return data

        self._list_cache.clear()
        self._list_cache.update(refreshed_list_cache)

        with contextlib.suppress(AttributeError):
            self._search_anime.cache_clear()

        return data

    @ttl_cache(ttl=3600)
    async def _get_user_anime_list_page(
        self,
        *,
        username: str = "@me",
        status: MalListStatus | str | None = None,
        limit: int = 1000,
        offset: int = 0,
        nsfw: bool = True,
        sort: str | None = None,
        fields: tuple[str, ...] | None = None,
    ) -> AnimePaging:
        """Cached helper that fetches one user anime-list page."""
        params: dict[str, Any] = {
            "limit": min(max(limit, 1), 1000),
            "offset": max(offset, 0),
            "nsfw": str(nsfw).lower(),
            "fields": (
                self.DEFAULT_ANIME_FIELDS_CSV if fields is None else ",".join(fields)
            ),
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
        return AnimePaging(**response)

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
        list_status = MyAnimeListStatus(**status_payload)
        if cached := self._cached(anime_id):
            cached.my_list_status = list_status
            self._list_cache[anime_id] = cached
        self._media_cache.clear()
        self._invalidate_cached_views()
        return list_status

    async def delete_anime_status(self, anime_id: int) -> None:
        """Remove a user's anime list entry."""
        if not self.access_token:
            raise aiohttp.ClientError("Access token is required to delete list entries")

        await self._make_request("DELETE", f"/anime/{anime_id}/my_list_status")
        self._list_cache.pop(anime_id, None)
        self._media_cache.pop(anime_id, None)
        self._invalidate_cached_views()

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
        try:
            async with (
                aiohttp.ClientSession(headers=headers) as session,
                session.post(TOKEN_URL, data=payload) as response,
            ):
                if response.status == 401:
                    raise aiohttp.ClientError(
                        "MAL API request unauthorized (401). "
                        "Verify your MAL client ID and refresh token."
                    )

                response.raise_for_status()
                data = await response.json()
        except aiohttp.ClientResponseError as exc:
            raise aiohttp.ClientError(
                "MAL token refresh failed. "
                f"status={exc.status}; "
                f"error={exc.message or str(exc)}"
            ) from exc
        except (aiohttp.ClientConnectionError, TimeoutError) as exc:
            raise aiohttp.ClientError(
                "MAL token refresh failed due to connection error. "
                f"error={exc.__class__.__name__}: {exc}"
            ) from exc

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", self.refresh_token)

        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

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
        """Make a rate-limited MAL API request with bounded retries."""
        max_attempts = 3
        if retry_count >= max_attempts:
            raise aiohttp.ClientError("Failed to make request after 3 tries")

        session = await self._get_session()
        url = f"{self.API_URL.rstrip('/')}/{path.lstrip('/')}"
        normalized_path = f"/{path.lstrip('/')}"

        for attempt in range(retry_count + 1, max_attempts + 1):
            try:
                await self._request_limiter.acquire()  # ty:ignore[invalid-await]

                async with session.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    data=data,
                ) as response:
                    if response.status == 401:
                        if not refresh_attempted and self.refresh_token:
                            await self.refresh_access_token()
                            session = await self._get_session()
                            refresh_attempted = True
                            continue

                        raise aiohttp.ClientError(
                            "MAL API request unauthorized (401). "
                            "Verify your MAL credentials."
                        )

                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After", "unknown")
                        raise aiohttp.ClientError(
                            f"MAL API rate limited (429). Retry-After: {retry_after}"
                        )

                    response.raise_for_status()

                    if response.status == 204:
                        return {}

                    return await response.json()

            except (
                aiohttp.ClientResponseError,
                aiohttp.ClientConnectionError,
                TimeoutError,
            ) as exc:
                if attempt < max_attempts:
                    self.log.error(
                        "Retrying failed request (attempt %s/%s): %s",
                        attempt,
                        max_attempts,
                        exc,
                    )
                    await asyncio.sleep(1)
                    continue

                error_message = (
                    exc.message
                    if isinstance(exc, aiohttp.ClientResponseError)
                    else str(exc)
                )

                raise aiohttp.ClientError(
                    "MAL request failed after 3 attempts. "
                    f"error={exc.__class__.__name__}: {error_message}; "
                    f"method={method}; "
                    f"path={normalized_path}; "
                    f"params={params}; "
                    f"json={json}"
                ) from exc

        raise aiohttp.ClientError("MAL request failed unexpectedly")

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
        except ValueError, IndexError:
            return None
