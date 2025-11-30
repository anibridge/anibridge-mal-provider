"""MAL client used by the AniBridge MAL list provider."""

import contextlib
import importlib.metadata
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any

import aiohttp
from async_lru import alru_cache
from limiter import Limiter

from anibridge_mal_provider.models import MalListStatus, MalMedia, MalUser

__all__ = ["MalClient"]

_LOG = getLogger(__name__)

mal_limiter = Limiter(rate=2, capacity=5, jitter=False)


class MalClient:
    """MyAnimeList client used by the AniBridge MAL list provider.

    Args:
        client_id (str): The MAL client ID.
        access_token (str | None): The OAuth access token.
        refresh_token (str | None): The OAuth refresh token.
        client_secret (str | None): The OAuth client secret.
    """

    API_BASE = "https://api.myanimelist.net/v2"
    TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"

    def __init__(
        self,
        client_id: str,
        access_token: str | None = None,
        refresh_token: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        """Initialize the MAL client.

        Args:
            client_id (str): The OAuth client ID.
            access_token (str | None): The OAuth access token.
            refresh_token (str | None): The OAuth refresh token.
            client_secret (str | None): The OAuth client secret.
        """
        self.client_id = client_id
        self._access_token = access_token
        self.refresh_token = refresh_token
        self.client_secret = client_secret

        self._access_token_expires_at: datetime | None = None
        self.offline_entries: dict[int, MalMedia] = {}
        self.user: MalUser | None = None

        self._session: aiohttp.ClientSession | None = None

    @property
    def is_authenticated(self) -> bool:
        """Return True if the client has a currently valid access token."""
        return self._access_token is not None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return an existing or new HTTP session for MAL requests."""
        if self._session is None or self._session.closed:
            version = importlib.metadata.version("anibridge-mal-provider")
            headers = {
                "X-MAL-Client-ID": self.client_id,
                "User-Agent": f"anibridge-mal-provider/{version}",
            }
            if self._access_token:
                headers["Authorization"] = f"Bearer {self._access_token}"
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def close(self) -> None:
        """Close all open HTTP sessions."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def initialize(self) -> None:
        """Prime the client caches by loading the MAL user profile."""
        self.offline_entries.clear()
        self.user = await self.get_user()

    async def get_user(self) -> MalUser:
        """Fetch the authenticated MAL user model."""
        payload = await self._request(
            "GET", "/users/@me", params={"fields": "anime_statistics"}
        )
        return MalUser(**payload)

    async def search_anime(
        self, query: str, *, limit: int = 10
    ) -> AsyncIterator[MalMedia]:
        """Search MAL for anime matching the provided query string.

        Args:
            query (str): Search query text.
            limit (int, optional): Maximum number of search results. Defaults to 10.

        Yields:
            MalMedia: Individual MAL anime search result.
        """
        payload: dict[str, Any] = await self._request(
            "GET",
            "/anime",
            params={
                "q": query,
                "limit": limit,
                "fields": ",".join(
                    [
                        "id",
                        "title",
                        "main_picture",
                        "alternative_titles",
                        "start_date",
                        "end_date",
                        "synopsis",
                        "mean",
                        "rank",
                        "popularity",
                        "num_list_users",
                        "media_type",
                        "status",
                        "genres",
                        "num_episodes",
                        "start_season",
                        "broadcast",
                        "source",
                        "average_episode_duration",
                        "rating",
                        "studios",
                        "my_list_status",
                    ]
                ),
            },
        )
        for entry in payload.get("data", []):
            node = entry.get("node", {})
            list_status = entry.get("list_status") or node.get("my_list_status")
            media = MalMedia(
                **{**node, "my_list_status": list_status} if list_status else node
            )
            yield media

    @alru_cache(maxsize=256)
    async def _fetch_anime(self, anime_id: int) -> MalMedia:
        """Fetch an anime from MAL and cache the result.

        Args:
            anime_id (int): MAL anime ID.

        Returns:
            MalMedia: Deserialized MAL anime object.
        """
        payload = await self._request(
            "GET",
            f"/anime/{anime_id}",
            params={"fields": ",".join(MalMedia.model_fields)},
        )
        media = MalMedia(**payload)
        self.offline_entries[media.id] = media
        return media

    async def get_anime(self, anime_id: int) -> MalMedia:
        """Return an anime, preferring cached values whenever possible."""
        if anime_id in self.offline_entries:
            return self.offline_entries[anime_id]
        return await self._fetch_anime(anime_id)

    async def update_anime_entry(
        self, anime_id: int, update_data: aiohttp.FormData
    ) -> None:
        """Persist list entry changes to MAL.

        Args:
            anime_id (int): The MAL anime ID whose list entry should be updated.
            update_data (aiohttp.FormData): The form data containing the updated fields.

        Raises:
            aiohttp.ClientError: If the update fails.
        """
        response = await self._request(
            "PATCH",
            f"/anime/{anime_id}/my_list_status",
            data=update_data,
        )

        status_model = MalListStatus(**response) if response else None
        media = self.offline_entries.get(anime_id)

        if media is None:
            media = await self._fetch_anime(anime_id)
            self.offline_entries[media.id] = media
        else:
            media.my_list_status = status_model

    async def delete_anime_entry(self, media_id: int) -> bool:
        """Delete the MAL list entry for the supplied anime id.

        Args:
            media_id (int): The MAL anime ID whose list entry should be deleted.

        Returns:
            bool: True if the deletion was successful.
        """
        await self._request("DELETE", f"/anime/{media_id}/my_list_status")
        with contextlib.suppress(KeyError):
            del self.offline_entries[media_id]
        return True

    async def backup_mal(self) -> str:
        """Export the authenticated user's MAL list in a portable JSON structure.

        Returns:
            str: The JSON serialized backup payload.
        """
        raise NotImplementedError

    async def restore_mal(self, backup: str) -> None:
        """Restore MAL entries from a JSON backup created by backup_mal.

        Args:
            backup (str): The JSON serialized backup payload.
        """
        raise NotImplementedError

    async def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        params: dict[str, Any] | None = None,
        json_payload: Any | None = None,
        data: Any | None = None,
        retry: bool = True,
    ) -> Any:
        """Send a rate limited request to MAL, refreshing tokens on demand."""
        session = await self._get_session()
        url = (
            path_or_url
            if path_or_url.startswith("http")
            else f"{self.API_BASE}{path_or_url}"
        )
        async with (
            mal_limiter,
            session.request(
                method, url, params=params, json=json_payload, data=data
            ) as response,
        ):
            if response.status == 401 and retry and self.refresh_token:
                await self._refresh_access_token()
                return await self._request(
                    method,
                    path_or_url,
                    params=params,
                    json_payload=json_payload,
                    data=data,
                    retry=False,
                )

            if response.status >= 400:
                message = await response.text()
                _LOG.error("MAL request failed: %s %s -- %s", method, url, message)
                response.raise_for_status()

            if response.status == 204:
                return None

            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                return await response.json()
            return await response.text()

    async def _refresh_access_token(self) -> None:
        """Refresh the OAuth access token when a refresh token is available."""
        if not self.refresh_token:
            raise aiohttp.ClientError(
                "Refresh token is required to refresh access tokens"
            )

        payload = {
            "client_id": self.client_id,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        if self.client_secret:
            payload["client_secret"] = self.client_secret

        async with (
            aiohttp.ClientSession() as session,
            session.post(self.TOKEN_URL, data=payload) as response,
        ):
            response.raise_for_status()
            token_response: dict[str, Any] = await response.json()

        self._access_token = token_response.get("access_token")
        self.refresh_token = token_response.get("refresh_token", self.refresh_token)
        expires_in = token_response.get("expires_in")
        if expires_in:
            self._access_token_expires_at = datetime.now(UTC) + timedelta(
                seconds=int(expires_in)
            )

        if self._session:
            await self._session.close()
            self._session = None
