"""Tests for the MAL API client."""

from datetime import date
from logging import getLogger
from types import SimpleNamespace
from typing import Any, cast

import aiohttp
import pytest
from anibridge.utils.types import ProviderLogger

from anibridge.providers.list.mal.client import TOKEN_URL, MalClient
from anibridge.providers.list.mal.models import Anime, MalListStatus


class _StubResponse:
    """Minimal aiohttp-like response context manager."""

    def __init__(
        self,
        *,
        status: int,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status = status
        self._payload = payload or {}
        self.headers = headers or {}
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self) -> dict[str, Any]:
        return self._payload

    async def text(self) -> str:
        return self._text

    def raise_for_status(self) -> None:
        if self.status >= 400 and self.status not in (401, 429, 502):
            raise aiohttp.ClientResponseError(
                request_info=cast(Any, SimpleNamespace(real_url="https://mal.example")),
                history=(),
                status=self.status,
                message="error",
            )


class _StubSession:
    """Session wrapper that serves predefined request responses."""

    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: Any):
        self.calls.append({"method": method, "url": url, **kwargs})
        next_item = self._responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item

    async def close(self) -> None:
        self.closed = True


@pytest.fixture()
def mal_client() -> MalClient:
    """Create a test MAL client with deterministic logger."""
    return MalClient(
        logger=cast(ProviderLogger, getLogger("tests.mal.client")),
        client_id="test-client-id",
        refresh_token="refresh-token",
    )


@pytest.mark.asyncio
async def test_initialize_sets_user_timezone_and_primes_cache(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """initialize should refresh auth, resolve user, and preload list cache."""
    calls: list[str] = []

    async def fake_refresh() -> None:
        calls.append("refresh")

    async def fake_get_user(username: str = "@me"):
        calls.append(f"user:{username}")
        return SimpleNamespace(id=1, name="Tester", time_zone="UTC")

    async def fake_fetch_list(**kwargs: Any):
        calls.append("list")
        return SimpleNamespace(data=[])

    mal_client._list_cache = {99: Anime(id=99, title="Old")}
    monkeypatch.setattr(mal_client, "refresh_access_token", fake_refresh)
    monkeypatch.setattr(mal_client, "get_user", fake_get_user)
    monkeypatch.setattr(mal_client, "_fetch_list_collection", fake_fetch_list)

    await mal_client.initialize()

    assert calls == ["refresh", "user:@me", "list"]
    assert mal_client.user is not None and mal_client.user.name == "Tester"
    assert mal_client.user_timezone is not None
    assert mal_client._list_cache == {}


@pytest.mark.asyncio
async def test_search_anime_clamps_limit_and_caches(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """search_anime should clamp limit and cache parsed anime payloads."""
    captured_params: dict[str, Any] = {}

    async def fake_make_request(
        method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        assert method == "GET"
        assert path == "/anime"
        captured_params.update(kwargs["params"])
        return {
            "data": [
                {
                    "node": {"id": 1, "title": "A"},
                    "list_status": {"status": "watching", "num_episodes_watched": 3},
                },
                {"node": {"id": 2, "title": "B"}},
            ]
        }

    monkeypatch.setattr(mal_client, "_make_request", fake_make_request)

    results = await mal_client.search_anime("query", limit=9999, nsfw=False)

    assert [anime.id for anime in results] == [1, 2]
    assert captured_params["limit"] == 100
    assert captured_params["nsfw"] == "false"
    assert set(mal_client._media_cache) == {1, 2}
    assert set(mal_client._list_cache) == {1}


@pytest.mark.asyncio
async def test_get_anime_cache_and_force_refresh(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_anime should serve cache unless force_refresh is requested."""
    mal_client._media_cache[7] = Anime(id=7, title="Cached")

    async def fake_fetch_list(**kwargs: Any):
        return SimpleNamespace(data=[])

    monkeypatch.setattr(mal_client, "_fetch_list_collection", fake_fetch_list)

    async def never_called(*args: Any, **kwargs: Any):
        raise AssertionError("_make_request should not be called for cached lookup")

    monkeypatch.setattr(mal_client, "_make_request", never_called)
    cached = await mal_client.get_anime(7)
    assert cached.title == "Cached"

    async def fake_make_request(
        method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        assert method == "GET"
        assert path == "/anime/7"
        return {"id": 7, "title": "Fresh"}

    monkeypatch.setattr(mal_client, "_make_request", fake_make_request)
    fresh = await mal_client.get_anime(7, force_refresh=True)
    assert fresh.title == "Fresh"
    assert mal_client._media_cache[7].title == "Fresh"


@pytest.mark.asyncio
async def test_get_user_anime_list_caches_entries(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_user_anime_list should parse payload and populate offline cache."""

    async def fake_make_request(
        method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        assert method == "GET"
        assert path == "/users/@me/animelist"
        assert kwargs["params"]["status"] == "watching"
        assert kwargs["params"]["sort"] == "list_updated_at"
        return {
            "data": [
                {
                    "node": {"id": 11, "title": "Eleven"},
                    "list_status": {"status": "watching", "score": 8},
                }
            ],
            "paging": {"next": "next-url"},
        }

    monkeypatch.setattr(mal_client, "_make_request", fake_make_request)
    paging = await mal_client.get_user_anime_list(
        status=MalListStatus.WATCHING,
        sort="list_updated_at",
    )

    assert len(paging.data) == 1
    assert paging.data[0].node.id == 11
    assert 11 in mal_client._media_cache
    assert 11 in mal_client._list_cache


@pytest.mark.asyncio
async def test_update_and_delete_anime_status_auth_and_payload(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """update/delete should require token and shape payloads correctly."""
    with pytest.raises(aiohttp.ClientError):
        await mal_client.update_anime_status(42, score=7)
    with pytest.raises(aiohttp.ClientError):
        await mal_client.delete_anime_status(42)

    mal_client.access_token = "access"
    mal_client._list_cache[42] = Anime(id=42, title="Before")
    mal_client._media_cache[42] = Anime(id=42, title="Before")

    captured: list[dict[str, Any]] = []

    async def fake_make_request(
        method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        captured.append({"method": method, "path": path, **kwargs})
        if method == "PATCH":
            return {
                "my_list_status": {
                    "status": "completed",
                    "score": 9,
                    "num_episodes_watched": 12,
                    "tags": ["fav"],
                }
            }
        return {}

    monkeypatch.setattr(mal_client, "_make_request", fake_make_request)

    status = await mal_client.update_anime_status(
        42,
        status=MalListStatus.COMPLETED,
        score=9,
        progress=12,
        is_rewatching=True,
        start_date=date(2024, 1, 1),
        finish_date=date(2024, 1, 5),
        num_times_rewatched=1,
        tags=["fav"],
        comments="done",
    )
    assert status.score == 9
    assert 42 in mal_client._list_cache
    assert mal_client._list_cache[42].my_list_status is not None
    assert mal_client._list_cache[42].my_list_status.score == 9
    assert mal_client._media_cache == {}
    assert captured[0]["method"] == "PATCH"
    assert captured[0]["data"]["status"] == "completed"
    assert captured[0]["data"]["num_watched_episodes"] == "12"

    await mal_client.delete_anime_status(42)
    assert captured[1]["method"] == "DELETE"


@pytest.mark.asyncio
async def test_make_request_handles_204_and_retry(mal_client: MalClient) -> None:
    """_make_request should retry on 429 and return empty dict for 204."""
    session = _StubSession(
        responses=[
            _StubResponse(status=429, headers={"Retry-After": "0"}),
            _StubResponse(status=204),
        ]
    )
    mal_client._session = cast(aiohttp.ClientSession, session)

    result = await mal_client._make_request("GET", "/anime/1")

    assert result == {}
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_make_request_refreshes_on_unauthorized(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_make_request should refresh token once on 401 then retry."""
    refreshed: list[bool] = []

    async def fake_refresh() -> None:
        refreshed.append(True)

    session = _StubSession(
        responses=[
            _StubResponse(status=401),
            _StubResponse(status=200, payload={"ok": True}),
        ]
    )
    mal_client._session = cast(aiohttp.ClientSession, session)
    mal_client.refresh_token = "refresh"
    monkeypatch.setattr(mal_client, "refresh_access_token", fake_refresh)

    result = await mal_client._make_request("GET", "/anime/1")

    assert result == {"ok": True}
    assert refreshed == [True]


@pytest.mark.asyncio
async def test_make_request_raises_after_three_retries(
    mal_client: MalClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_make_request should stop retrying after the configured retry limit."""
    mal_client._session = cast(
        aiohttp.ClientSession,
        _StubSession(responses=[TimeoutError("timeout")]),
    )

    async def fast_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("anibridge.providers.list.mal.client.asyncio.sleep", fast_sleep)

    with pytest.raises(aiohttp.ClientError):
        await mal_client._make_request("GET", "/anime/1", retry_count=3)


@pytest.mark.asyncio
async def test_refresh_access_token_updates_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """refresh_access_token should store access token and rotate refresh token."""
    client = MalClient(
        logger=cast(ProviderLogger, getLogger("tests.mal.client.refresh")),
        client_id="client-id",
        refresh_token="old-refresh",
    )

    class _TokenSession:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def post(self, url: str, data: dict[str, str]):
            assert url == TOKEN_URL
            assert data["grant_type"] == "refresh_token"
            return _StubResponse(
                status=200,
                payload={"access_token": "new-access", "refresh_token": "new-refresh"},
            )

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        "anibridge.providers.list.mal.client.aiohttp.ClientSession", _TokenSession
    )

    await client.refresh_access_token()

    assert client.access_token == "new-access"
    assert client.refresh_token == "new-refresh"


def test_parse_date_variants() -> None:
    """parse_date should gracefully parse multiple MAL date formats."""
    assert MalClient.parse_date(None) is None
    assert MalClient.parse_date("") is None
    assert MalClient.parse_date(date(2024, 1, 2)) == date(2024, 1, 2)
    assert MalClient.parse_date("2024-03-05") == date(2024, 3, 5)
    assert MalClient.parse_date("2024-03") == date(2024, 3, 1)
    assert MalClient.parse_date("2024") == date(2024, 1, 1)
    assert MalClient.parse_date("bad-value") is None
    assert MalClient.parse_date(12345) is None


@pytest.mark.asyncio
async def test_close_closes_active_session(mal_client: MalClient) -> None:
    """close should close the active session when present."""

    class _Closable:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    session = _Closable()
    mal_client._session = session  # ty:ignore[invalid-assignment]

    await mal_client.close()
    assert session.closed is True
