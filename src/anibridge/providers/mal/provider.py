"""AniBridge provider implementation for MyAnimeList."""

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from json import JSONDecodeError
from typing import Any, cast

import aiohttp
import msgspec
from anibridge.provider.base import (
    Account,
    Artwork,
    BackupArtifact,
    Capabilities,
    DeleteRecord,
    Descriptor,
    ExternalId,
    FacetName,
    FieldSpec,
    Match,
    Node,
    NodeKind,
    NodeQuery,
    NodeSpec,
    NumericConstraint,
    Page,
    Progress,
    ProgressConstraint,
    Provider,
    Rating,
    Record,
    RecordField,
    RecordQuery,
    RecordSpec,
    RecordWrite,
    Ref,
    Role,
    State,
    Status,
    SupportsBackupExports,
    SupportsBackupImports,
    SupportsMapping,
    SupportsNodeReads,
    SupportsNodeSearch,
    SupportsRecordReads,
    SupportsRecordWrites,
    TemporalConstraint,
    TemporalPrecision,
    TextConstraint,
    UpsertRecord,
    Value,
    WriteError,
    WriteOp,
    WriteResult,
)

from anibridge.providers.mal.client import MalClient
from anibridge.providers.mal.config import MalProviderConfig
from anibridge.providers.mal.models import Anime, MalListStatus, MyAnimeListStatus

__all__ = ["MalProvider"]

_ANIME_LIST_SURFACE = "anime_list"

_STATUS_TO_NATIVE: dict[Status, MalListStatus] = {
    Status.ACTIVE: MalListStatus.WATCHING,
    Status.PLANNED: MalListStatus.PLAN_TO_WATCH,
    Status.COMPLETED: MalListStatus.COMPLETED,
    Status.DROPPED: MalListStatus.DROPPED,
    Status.PAUSED: MalListStatus.ON_HOLD,
    Status.REPEATING: MalListStatus.WATCHING,
}
_NATIVE_TO_STATUS: dict[MalListStatus, Status] = {
    native: status
    for status, native in _STATUS_TO_NATIVE.items()
    if status is not Status.REPEATING
}


class MalProvider(
    Provider,
    SupportsMapping,
    SupportsNodeReads,
    SupportsNodeSearch,
    SupportsRecordReads,
    SupportsRecordWrites,
    SupportsBackupExports,
    SupportsBackupImports,
):
    """MAL target provider for the AniBridge provider contract."""

    DISPLAY_NAME = "MyAnimeList"
    NAMESPACE = "mal"

    def __init__(
        self,
        *,
        logger,
        config: Mapping[str, object] | None = None,
    ) -> None:
        """Parse configuration and prepare the MAL client."""
        super().__init__(logger=logger, config=config)
        self.parsed_config = msgspec.convert(config or {}, type=MalProviderConfig)
        self._client = MalClient(
            logger=self.log,
            client_id=self.parsed_config.client_id,
            refresh_token=self.parsed_config.token,
            rate_limit=self.parsed_config.rate_limit,
        )
        self._account: Account | None = None

    async def initialize(self) -> None:
        """Initialize the MAL API session and user cache."""
        self.log.debug("Initializing MAL provider client")
        await self._client.initialize()
        if self._client.user is None:
            raise RuntimeError("MAL provider initialized without a resolved user")
        self._account = Account(
            key=str(self._client.user.id),
            title=self._client.user.name,
            url=f"https://myanimelist.net/profile/{self._client.user.name}",
        )
        self.log.debug("MAL provider initialized for user id=%s", self._account.key)

    def account(self) -> Account | None:
        """Return the connected MAL account."""
        return self._account

    def capabilities(self) -> Capabilities:
        """Advertise MAL target capabilities."""
        return Capabilities(
            roles=frozenset({Role.TARGET}),
            facets=frozenset({FacetName.ARTWORK}),
            nodes=(NodeSpec(Descriptor("anime", NodeKind.SERIES)),),
            records=(
                RecordSpec(
                    surface=_ANIME_LIST_SURFACE,
                    fields={
                        RecordField.STATUS: FieldSpec(
                            RecordField.STATUS,
                            readable=True,
                            writable=True,
                            values=tuple(
                                Descriptor(native.value, status)
                                for status, native in _STATUS_TO_NATIVE.items()
                            ),
                        ),
                        RecordField.PROGRESS: FieldSpec(
                            RecordField.PROGRESS,
                            readable=True,
                            writable=True,
                            constraints=(
                                ProgressConstraint(
                                    current=NumericConstraint(0, None, 1),
                                    total=False,
                                    unit=False,
                                ),
                            ),
                        ),
                        RecordField.RATING: FieldSpec(
                            RecordField.RATING,
                            readable=True,
                            writable=True,
                            constraints=(NumericConstraint(0, 10, 1),),
                        ),
                        RecordField.STARTED_AT: FieldSpec(
                            RecordField.STARTED_AT,
                            readable=True,
                            writable=True,
                            constraints=(
                                TemporalConstraint(precision=TemporalPrecision.DATE),
                            ),
                        ),
                        RecordField.FINISHED_AT: FieldSpec(
                            RecordField.FINISHED_AT,
                            readable=True,
                            writable=True,
                            constraints=(
                                TemporalConstraint(precision=TemporalPrecision.DATE),
                            ),
                        ),
                        RecordField.REPEAT_COUNT: FieldSpec(
                            RecordField.REPEAT_COUNT,
                            readable=True,
                            writable=True,
                            constraints=(NumericConstraint(0, None, 1),),
                        ),
                        RecordField.NOTES: FieldSpec(
                            RecordField.NOTES,
                            readable=True,
                            writable=True,
                            constraints=(TextConstraint(max_length=65535),),
                        ),
                    },
                    write_ops=frozenset({WriteOp.UPSERT_RECORD, WriteOp.DELETE_RECORD}),
                ),
            ),
            external_authorities=frozenset({self.NAMESPACE}),
        )

    async def close(self) -> None:
        """Close the MAL API session."""
        await self._client.close()

    async def clear_cache(self) -> None:
        """Clear MAL provider caches."""
        self._client.clear_cache()

    async def export_backup(self) -> BackupArtifact | None:
        """Export the MAL list as a provider-managed backup artifact."""
        entries: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await self._client.get_user_anime_list(offset=offset, limit=1000)
            for item in page.data:
                status = item.list_status or item.node.my_list_status
                if status is None:
                    continue
                values = msgspec.json.decode(msgspec.json.encode(status))
                entries.append(
                    {
                        "id": item.node.id,
                        **{
                            key: value
                            for key, value in values.items()
                            if value is not None
                        },
                    }
                )
            if page.paging is None or page.paging.next is None:
                break
            offset += 1000

        return BackupArtifact(
            content=json.dumps(entries, separators=(",", ":")).encode(),
            file_extension=".json",
            media_type="application/json",
        )

    async def import_backup(self, payload: bytes) -> None:
        """Restore MAL list entries from a provider-managed backup artifact."""
        try:
            data = json.loads(payload.decode())
        except JSONDecodeError:
            self.log.exception("Failed to decode MAL backup JSON")
            raise

        restore_ids = {int(item["id"]) for item in data}
        existing_ids: set[int] = set()
        offset = 0
        while True:
            page = await self._client.get_user_anime_list(offset=offset, limit=1000)
            existing_ids.update(item.node.id for item in page.data)
            if page.paging is None or page.paging.next is None:
                break
            offset += 1000

        for item in data:
            anime_id = int(item.pop("id"))
            status = msgspec.convert(item, type=MyAnimeListStatus)
            await self._client.update_anime_status(
                anime_id=anime_id,
                status=status.status,
                score=status.score,
                progress=status.num_episodes_watched,
                is_rewatching=status.is_rewatching,
                start_date=cast(date | None, status.start_date),
                finish_date=cast(date | None, status.finish_date),
                priority=status.priority,
                num_times_rewatched=status.num_times_rewatched,
                rewatch_value=status.rewatch_value,
                tags=cast(Sequence[str], status.tags),
                comments=status.comments,
            )
        for anime_id in existing_ids - restore_ids:
            await self._client.delete_anime_status(anime_id)

    async def resolve(self, ids: Sequence[ExternalId]) -> Sequence[Match]:
        """Resolve MAL external IDs to MAL refs."""
        matches: list[Match] = []
        for external_id in ids:
            if external_id.authority != self.NAMESPACE:
                continue
            try:
                int(external_id.value)
            except ValueError:
                continue
            matches.append(
                Match(
                    external_id=external_id,
                    ref=Ref.anchor(external_id.value),
                    confidence=1.0,
                )
            )
        return tuple(matches)

    async def fetch_nodes(self, query: NodeQuery) -> Page[Node]:
        """Fetch MAL anime metadata for targeted refs."""
        if query.native_node_kinds and "anime" not in query.native_node_kinds:
            return Page(items=())

        nodes: list[Node] = []
        for ref in query.refs:
            if query.limit is not None and len(nodes) >= query.limit:
                break
            try:
                anime = await self._client.get_anime(int(ref.key))
            except ValueError:
                self.log.warning("Invalid MAL media ref %s", ref.key)
                continue
            nodes.append(self._node_from_anime(anime, query.facets))
        return Page(items=tuple(nodes))

    async def search_nodes(
        self,
        query: str,
        *,
        limit: int = 10,
        facets: frozenset[FacetName] = frozenset(),
    ) -> Page[Node]:
        """Search MAL anime by title."""
        text = query.strip()
        if not text:
            return Page(items=())
        anime_items = await self._client.search_anime(text, limit=limit)
        return Page(
            items=tuple(self._node_from_anime(anime, facets) for anime in anime_items)
        )

    async def fetch_records(self, query: RecordQuery) -> Page[Record]:
        """Fetch MAL anime-list records by ref or record key."""
        refs = tuple(query.refs)
        if not refs and query.keys:
            refs = tuple(Ref.anchor(key) for key in query.keys)
        if (
            query.record_surfaces
            and _ANIME_LIST_SURFACE not in query.record_surfaces
        ):
            return Page(items=())

        records: list[Record] = []
        for ref in refs:
            if query.limit is not None and len(records) >= query.limit:
                break
            try:
                anime = await self._client.get_anime(int(ref.key))
            except ValueError:
                self.log.warning("Invalid MAL media ref %s", ref.key)
                continue
                records.append(self._record_from_anime(anime, query.fields))
        return Page(items=tuple(records))

    async def write_records(
        self,
        writes: Sequence[RecordWrite],
    ) -> Sequence[WriteResult]:
        """Apply MAL record writes."""
        results: list[WriteResult] = []
        for write in writes:
            try:
                if isinstance(write, UpsertRecord):
                    result = await self._upsert_record(write)
                else:
                    result = await self._delete_record(write)
            except Exception as exc:
                op = (
                    WriteOp.DELETE_RECORD
                    if isinstance(write, DeleteRecord)
                    else WriteOp.UPSERT_RECORD
                )
                result = WriteResult(
                    ok=False,
                    op=op,
                    token=write.token,
                    code=self._write_error_for_exception(exc),
                    error=str(exc),
                    ref=write.ref,
                )
            results.append(result)
        return tuple(results)

    def _record_from_anime(
        self,
        anime: Anime,
        fields: frozenset[RecordField],
    ) -> Record:
        """Convert MAL anime/list state into a contract record."""
        requested = fields or frozenset(RecordField)
        status = anime.my_list_status
        values: dict[RecordField, Value] = {}
        if status is not None:
            if RecordField.STATUS in requested and status.status is not None:
                values[RecordField.STATUS] = self._state_from_status(status)
            if (
                RecordField.PROGRESS in requested
                and status.num_episodes_watched is not None
            ):
                values[RecordField.PROGRESS] = Progress(
                    current=status.num_episodes_watched,
                    total=anime.num_episodes,
                    unit="episode",
                )
            if RecordField.RATING in requested and status.score is not None:
                values[RecordField.RATING] = Rating(float(status.score), (0, 10, 1))
            if (
                RecordField.REPEAT_COUNT in requested
                and status.num_times_rewatched is not None
            ):
                values[RecordField.REPEAT_COUNT] = status.num_times_rewatched
            if RecordField.NOTES in requested and status.comments:
                values[RecordField.NOTES] = status.comments
            if RecordField.STARTED_AT in requested and status.start_date is not None:
                values[RecordField.STARTED_AT] = cast(date, status.start_date)
            if RecordField.FINISHED_AT in requested and status.finish_date is not None:
                values[RecordField.FINISHED_AT] = cast(date, status.finish_date)

        return Record(
            ref=Ref.anchor(str(anime.id)),
            surface=_ANIME_LIST_SURFACE,
            key=str(anime.id),
            ids=(ExternalId(self.NAMESPACE, str(anime.id)),),
            values=values,
            updated_at=(
                cast(datetime, status.updated_at).astimezone(UTC)
                if status is not None and isinstance(status.updated_at, datetime)
                else None
            ),
            url=f"https://myanimelist.net/anime/{anime.id}",
        )

    def _node_from_anime(
        self,
        anime: Anime,
        facets: frozenset[FacetName],
    ) -> Node:
        """Convert MAL anime metadata into a contract node."""
        hydrated = {}
        if FacetName.ARTWORK in facets and anime.main_picture is not None:
            poster = anime.main_picture.large or anime.main_picture.medium
            if poster:
                hydrated[FacetName.ARTWORK] = Artwork({"poster": poster})

        labels: list[str] = []
        if anime.start_season and anime.start_season.year:
            season = anime.start_season.season
            if season:
                labels.append(f"{season.title()} {anime.start_season.year}")
            else:
                labels.append(str(anime.start_season.year))
        if anime.media_type:
            labels.append(anime.media_type.replace("_", " ").title())
        if anime.status:
            labels.append(anime.status.replace("_", " ").title())

        return Node(
            ref=Ref.anchor(str(anime.id)),
            kind="anime",
            title=anime.title,
            url=f"https://myanimelist.net/anime/{anime.id}",
            labels=tuple(labels),
            facets=hydrated,
        )

    async def _upsert_record(self, write: UpsertRecord) -> WriteResult:
        """Apply one upsert record write."""
        anime_id = int(write.ref.key)
        current = await self._client.get_anime(anime_id)
        current_status = current.my_list_status or MyAnimeListStatus()
        status = current_status.status
        score = current_status.score
        progress = current_status.num_episodes_watched
        is_rewatching = current_status.is_rewatching
        start_date = cast(date | None, current_status.start_date)
        finish_date = cast(date | None, current_status.finish_date)
        repeats = current_status.num_times_rewatched
        comments = current_status.comments

        for field in write.clear:
            if field is RecordField.STATUS:
                status = None
                is_rewatching = None
            elif field is RecordField.PROGRESS:
                progress = 0
            elif field is RecordField.RATING:
                score = 0
            elif field is RecordField.STARTED_AT:
                start_date = None
            elif field is RecordField.FINISHED_AT:
                finish_date = None
            elif field is RecordField.REPEAT_COUNT:
                repeats = 0
            elif field is RecordField.NOTES:
                comments = ""

        for field, value in write.set.items():
            if field is RecordField.STATUS:
                status_value = value.status if isinstance(value, State) else value
                if not isinstance(status_value, Status):
                    raise ValueError("status must be a Status value")
                status = _STATUS_TO_NATIVE[status_value]
                is_rewatching = status_value is Status.REPEATING
            elif field is RecordField.PROGRESS:
                progress = self._numeric_value(field, value, integer=True)
            elif field is RecordField.RATING:
                score = self._numeric_value(field, value, integer=True)
            elif field is RecordField.STARTED_AT:
                start_date = self._date_value(field, value)
            elif field is RecordField.FINISHED_AT:
                finish_date = self._date_value(field, value)
            elif field is RecordField.REPEAT_COUNT:
                repeats = self._numeric_value(field, value, integer=True)
            elif field is RecordField.NOTES:
                comments = str(value)
            else:
                raise ValueError(f"MAL cannot write field {field.value!r}")

        saved = await self._client.update_anime_status(
            anime_id=anime_id,
            status=status,
            score=score,
            progress=progress,
            is_rewatching=is_rewatching,
            start_date=start_date,
            finish_date=finish_date,
            num_times_rewatched=repeats,
            comments=comments,
            tags=cast(Sequence[str], current_status.tags),
        )
        current.my_list_status = saved
        return WriteResult(
            ok=True,
            op=WriteOp.UPSERT_RECORD,
            token=write.token,
            key=str(anime_id),
            ref=write.ref,
            revision=(
                saved.updated_at.isoformat()
                if isinstance(saved.updated_at, datetime)
                else None
            ),
        )

    async def _delete_record(self, write: DeleteRecord) -> WriteResult:
        """Delete one MAL record."""
        ref = write.ref
        if ref is None:
            return WriteResult(
                ok=False,
                op=WriteOp.DELETE_RECORD,
                token=write.token,
                code=WriteError.INVALID,
                error="MAL delete requires a ref",
            )
        await self._client.delete_anime_status(int(ref.key))
        return WriteResult(
            ok=True,
            op=WriteOp.DELETE_RECORD,
            token=write.token,
            ref=ref,
            key=ref.key,
        )

    @staticmethod
    def _state_from_status(status: MyAnimeListStatus) -> State:
        """Convert MAL native status into contract state."""
        normalized = Status.REPEATING if status.is_rewatching else None
        if normalized is None and status.status is not None:
            normalized = _NATIVE_TO_STATUS.get(status.status)
        if normalized is None and status.status is None:
            return State(native="unknown")
        return State(
            native=status.status.value if status.status is not None else None,
            status=normalized,
        )

    @staticmethod
    def _date_value(field: RecordField, value: object) -> date:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field.value} must be timezone-aware")
            return value.astimezone(UTC).date()
        if isinstance(value, date):
            return value
        raise ValueError(f"{field.value} must be date")

    @staticmethod
    def _numeric_value(field: RecordField, value: object, *, integer: bool) -> int:
        if isinstance(value, Progress):
            raw_value = value.current or 0
        elif isinstance(value, Rating):
            raw_value = value.value
        else:
            raw_value = value
        if not isinstance(raw_value, int | float) or isinstance(raw_value, bool):
            raise ValueError(f"{field.value} must be numeric")
        return int(raw_value) if integer else round(raw_value)

    @staticmethod
    def _write_error_for_exception(exc: Exception) -> WriteError:
        """Classify an exception into a contract write error."""
        if isinstance(exc, ValueError):
            return WriteError.INVALID
        if isinstance(exc, aiohttp.ClientResponseError):
            if exc.status in {401, 403}:
                return WriteError.AUTH
            if exc.status == 404:
                return WriteError.NOT_FOUND
            if exc.status == 429:
                return WriteError.RATE_LIMITED
        if isinstance(exc, aiohttp.ClientError):
            return WriteError.TRANSIENT
        return WriteError.INTERNAL
