"""
Multi-database manager for the media store.

Design (Phase 3 of the branch task):
  * The bot may be configured with several MongoDB nodes (databases).
  * One node is the "active" node: all new writes go there.
  * When the active node reaches its configured capacity, or a write to it
    fails in a way that proves the node cannot accept new data, the manager
    switches to the next available node — automatically and concurrency-safely.
  * Reads fall back across all nodes (handled in database/ia_filterdb.py), so
    previously written data stays reachable no matter which node holds it.
  * The active-node choice is persisted in a small state collection on the
    primary node, so restarts don't flip back to a full node.

Environment variables (all optional, sane defaults):
  DATABASE_URI / DATABASE_NAME / COLLECTION_NAME       primary node (from info.py, untouched)
  SECONDARY_DATABASE_URIS   "uri1, uri2, ..."          additional media nodes
  SECONDARY_DATABASE_NAMES  "name1, name2, ..."        DB names for the secondaries
  SECONDARY_COLLECTION_NAME "name"                     collection used on secondaries (defaults to COLLECTION_NAME)
  DATABASE_MAX_BYTES        integer                    per-node capacity in bytes; when > 0 the active
                                                       node's data size is enforced before writes.
"""

import asyncio
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration (info.py stays untouched; everything new comes from the env)
# ---------------------------------------------------------------------------

try:
    from os import environ as _environ
except ImportError:  # pragma: no cover
    _environ = {}


def _env_list(name: str) -> List[str]:
    raw = _environ.get(name, "") or ""
    return [part.strip() for part in raw.split(",") if part.strip()]


def _env_int(name: str, default: int = 0) -> int:
    raw = (_environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid integer for %s=%r; using default %s", name, raw, default)
        return default


def _mask_uri(uri: str) -> str:
    """Hide credentials in a MongoDB URI before logging it."""
    if not uri:
        return ""
    m = re.match(r"^(mongodb(?:\+srv)?://)([^/?#]+)(.*)$", uri)
    if m:
        userinfo = m.group(2)
        if "@" in userinfo:
            userinfo = userinfo.split("@", 1)[1]
        return f"{m.group(1)}{userinfo}{m.group(3)}"
    return uri


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

_CAPACITY_ERRORS: tuple = ()
_WRITE_ERRORS: tuple = ()
_MONGO_BASE: Any = Exception
_ATLAS_QUOTA_CODES = {8000, 8002}  # Atlas free-tier storage / ops quota

try:  # pymongo is a hard dependency of the real bot
    from pymongo.errors import (  # type: ignore
        AutoReconnect,
        ConnectionFailure,
        DocumentTooLarge,
        InvalidDocument,
        NetworkTimeout,
        NotPrimaryError,
        OperationFailure,
        ProtocolError,
        ServerSelectionTimeoutError,
        TimeoutError as PymongoTimeoutError,
        WTimeoutError,
        WriteError,
        WriteConcernError,
    )

    _CAPACITY_ERRORS = (DocumentTooLarge, InvalidDocument)
    _WRITE_ERRORS = (
        AutoReconnect,
        ConnectionFailure,
        NetworkTimeout,
        ProtocolError,
        ServerSelectionTimeoutError,
        PymongoTimeoutError,
        WTimeoutError,
        NotPrimaryError,
        WriteError,
        WriteConcernError,
    )
    _MONGO_BASE = OperationFailure
except ImportError:  # pragma: no cover - keeps the module importable in tests
    AutoReconnect = ConnectionFailure = ServerSelectionTimeoutError = Exception
    WriteError = WTimeoutError = WriteConcernError = NotPrimaryError = Exception

    class _OpFail(Exception):  # minimal stand-in for OperationFailure
        def __init__(self, error=None, code=None, *args, **kwargs):
            super().__init__(error, *args, **kwargs)
            self.code = code

    OperationFailure = _OpFail


def _operation_code(exc: BaseException) -> Optional[int]:
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    details = getattr(exc, "details", None)
    if isinstance(details, dict):
        inner = details.get("code") or (details.get("writeError") or {}).get("code")
        if isinstance(inner, int):
            return inner
    return None


class AllDatabasesUnavailable(RuntimeError):
    """Raised when no configured database node can be reached."""


# ---------------------------------------------------------------------------
# Node wrapper
# ---------------------------------------------------------------------------

class DatabaseNode:
    """One MongoDB node (a cluster URI + database + media collection)."""

    def __init__(self, index: int, uri: str, db_name: str, collection_name: str):
        self.index = index
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.label = f"DB-{index + 1}({_mask_uri(uri)}/{db_name})"
        self.client = None
        self.db = None
        self.collection = None

    # -- lifecycle ----------------------------------------------------------

    def connect(self) -> bool:
        """
        Construct the motor client (does not hit the network; motor connects
        lazily). Catches invalid-URI style configuration errors.
        Use :meth:`check` for a real connectivity probe.
        """
        if self.client is not None:
            return True
        try:
            from motor.motor_asyncio import AsyncIOMotorClient

            self.client = AsyncIOMotorClient(
                self.uri,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                retryWrites=True,
            )
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info("Database node %s configured", self.label)
            return True
        except Exception as exc:
            logger.error("Database node %s configuration error: %s", self.label, exc)
            self.client = None
            self.db = None
            self.collection = None
            return False

    async def check(self) -> bool:
        """Real connectivity probe (ping with timeout)."""
        if self.client is None:
            return False
        try:
            await asyncio.wait_for(self.client.admin.command("ping"), timeout=10)
            return True
        except Exception as exc:
            logger.warning("Database node %s failed connectivity check: %s", self.label, exc)
            return False

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:  # pragma: no cover - close is best-effort
                pass
        self.client = None
        self.db = None
        self.collection = None

    # -- metrics ------------------------------------------------------------

    async def data_size(self) -> int:
        """Logical data size in bytes (dbstats().dataSize)."""
        if self.db is None:
            return 0
        try:
            stats = await self.db.command("dbstats")
            return int(stats.get("dataSize", 0) or 0)
        except Exception as exc:
            logger.warning("Could not read dbstats for %s: %s", self.label, exc)
            return 0

    def is_full(self, size: int, limit: int) -> bool:
        return limit > 0 and size >= limit


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class MultiDatabaseManager:
    """Owns every configured node; picks the active one and fails over safely."""

    STATE_KEY = "active_db_index"
    STATE_COLLECTION = "db_manager_state"
    SIZE_CACHE_TTL = 60.0  # seconds

    def __init__(
        self,
        primary_uri: str,
        primary_db: str,
        primary_collection: str,
        secondary_uris: Sequence[str] = (),
        secondary_dbs: Optional[Sequence[str]] = None,
        secondary_collection: Optional[str] = None,
        max_bytes: int = 0,
    ):
        if not primary_uri:
            raise ValueError("A primary DATABASE_URI is required")

        secondary_uris = list(secondary_uris)
        secondary_dbs = list(secondary_dbs or [])
        while len(secondary_dbs) < len(secondary_uris):
            secondary_dbs.append(primary_db)

        self.max_bytes = max_bytes
        self.nodes: List[DatabaseNode] = [
            DatabaseNode(0, primary_uri, primary_db, primary_collection)
        ]
        for i, uri in enumerate(secondary_uris):
            self.nodes.append(
                DatabaseNode(
                    i + 1,
                    uri,
                    secondary_dbs[i],
                    secondary_collection or primary_collection,
                )
            )

        self._switch_lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        self._initialized = False
        self.active_index: int = 0
        self._switch_count = 0
        self.last_switch_reason: Optional[str] = None
        self._switch_listeners: List[Callable[..., Any]] = []
        self._size_cache: Dict[int, tuple] = {}  # index -> (monotonic_ts, bytes)

    # -- helpers ------------------------------------------------------------

    @property
    def active(self) -> DatabaseNode:
        return self.nodes[self.active_index]

    def node_count(self) -> int:
        return len(self.nodes)

    def add_switch_listener(self, fn: Callable[..., Any]) -> None:
        """Register a sync or async callback fired after every node switch."""
        self._switch_listeners.append(fn)

    def _classify(self, exc: BaseException) -> str:
        if isinstance(exc, _CAPACITY_ERRORS):
            return "capacity"
        if isinstance(exc, _MONGO_BASE):
            code = _operation_code(exc)
            if code in _ATLAS_QUOTA_CODES:
                return "capacity"
            msg = str(exc).lower()
            if "quota" in msg or "storage limit" in msg or "over quota" in msg:
                return "capacity"
        if isinstance(exc, _WRITE_ERRORS):
            return "unavailable"
        return "unknown"

    def _write_indicates_full_node(self, exc: BaseException) -> bool:
        """
        True when the exception proves the active node cannot accept NEW data
        (capacity or hard unavailability) and switching is the right response.
        """
        kind = self._classify(exc)
        if kind in ("capacity", "unavailable"):
            return True
        # Unknown OperationFailure codes may still be quota errors on managed
        # providers; only treat them as fatal when the message says so.
        if isinstance(exc, _MONGO_BASE):
            msg = str(exc).lower()
            return "quota" in msg or "storage" in msg or "limit" in msg
        return False

    # -- startup ------------------------------------------------------------

    async def initialize(self) -> None:
        """
        Connect to every configured node, probe connectivity, and restore the
        persisted active index. Idempotent.

        Raises AllDatabasesUnavailable only when NO node answers; partial
        availability is fine (the bot runs on whichever nodes answered).
        """
        async with self._init_lock:
            if self._initialized:
                return

            connected: List[int] = []
            for node in self.nodes:
                if node.connect() and await node.check():
                    connected.append(node.index)

            if not connected:
                logger.critical(
                    "No database node could be reached! Configured nodes: %s",
                    ", ".join(n.label for n in self.nodes),
                )
                raise AllDatabasesUnavailable("No database node could be reached")

            restored = await self._load_persisted_index()
            if restored is not None and restored in connected:
                self.active_index = restored
                logger.info("Restored active database: %s", self.active.label)
            else:
                if restored is not None:
                    logger.warning(
                        "Persisted active DB index %s is unreachable; using %s instead",
                        restored, connected[0],
                    )
                self.active_index = connected[0]
                await self._persist_index(self.active_index)

            self._initialized = True
            logger.info(
                "Multi-database manager ready: %d node(s), active=%s, capacity limit=%s",
                len(self.nodes),
                self.active.label,
                f"{self.max_bytes} bytes" if self.max_bytes > 0 else "unlimited",
            )

    async def ensure_initialized(self) -> None:
        """Lazily initialize on first use (safe to call from many tasks)."""
        if not self._initialized:
            await self.initialize()

    async def _load_persisted_index(self) -> Optional[int]:
        primary = self.nodes[0]
        if primary.db is None:
            return None
        try:
            coll = primary.db[self.STATE_COLLECTION]
            doc = await coll.find_one({"_id": self.STATE_KEY})
            if doc and isinstance(doc.get("index"), int):
                return doc["index"]
        except Exception as exc:
            logger.warning("Could not read persisted active-db state: %s", exc)
        return None

    async def _persist_index(self, index: int) -> None:
        primary = self.nodes[0]
        if primary.db is None:
            return
        try:
            coll = primary.db[self.STATE_COLLECTION]
            await coll.update_one(
                {"_id": self.STATE_KEY}, {"$set": {"index": index}}, upsert=True
            )
        except Exception as exc:
            # Non-fatal: the in-memory choice still governs this process.
            logger.warning("Could not persist active-db state: %s", exc)

    # -- switching ----------------------------------------------------------

    async def select_next_available(self, reason: str) -> bool:
        """
        Switch to the next reachable node that is not over capacity.
        Concurrency-safe: racing callers coalesce on one switch.

        Returns True when the active node changed.
        """
        async with self._switch_lock:
            current = self.active_index
            for node in self.nodes:
                if node.index == current:
                    continue
                if not await node.check():
                    logger.warning("Failover skipped %s: unreachable", node.label)
                    continue
                if self.max_bytes > 0:
                    size = await self._size(node, force=True)
                    if node.is_full(size, self.max_bytes):
                        logger.warning(
                            "Failover skipped %s: full (%d/%d bytes)",
                            node.label, size, self.max_bytes,
                        )
                        continue
                previous = self.active
                self.active_index = node.index
                self._switch_count += 1
                self.last_switch_reason = reason
                await self._persist_index(node.index)
                logger.warning(
                    "Switching active database %s -> %s (reason: %s)",
                    previous.label, node.label, reason,
                )
                await self._fire_listeners(previous, node, reason)
                return True
            logger.error(
                "Database failover failed (%s): no alternative node available; "
                "keeping %s as active", reason, self.active.label,
            )
            return False

    async def _fire_listeners(self, previous: DatabaseNode, new: DatabaseNode, reason: str) -> None:
        for fn in self._switch_listeners:
            try:
                result = fn(previous, new, reason)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Switch listener %r failed", fn)

    async def handle_write_failure(self, exc: BaseException) -> bool:
        """Classify a write failure and switch nodes when that can help."""
        if not self._write_indicates_full_node(exc):
            return False
        kind = self._classify(exc)
        return await self.select_next_available(f"write failure ({kind}): {exc}")

    # -- capacity -----------------------------------------------------------

    async def _size(self, node: DatabaseNode, force: bool = False) -> int:
        now = time.monotonic()
        cached = self._size_cache.get(node.index)
        if not force and cached and (now - cached[0]) < self.SIZE_CACHE_TTL:
            return cached[1]
        size = await node.data_size()
        self._size_cache[node.index] = (now, size)
        return size

    async def ensure_capacity_ok(self) -> bool:
        """
        Make sure the active node can still accept a write under the
        configured byte limit. Switches nodes when the limit is reached.
        Returns True when the active node is usable.
        """
        if self.max_bytes <= 0:
            return True
        size = await self._size(self.active)
        if not self.active.is_full(size, self.max_bytes):
            return True
        logger.warning(
            "Active database %s reached capacity (%d/%d bytes)",
            self.active.label, size, self.max_bytes,
        )
        return await self.select_next_available(
            f"capacity limit reached ({size}/{self.max_bytes} bytes)"
        )

    # -- status -------------------------------------------------------------

    async def status_report(self) -> Dict[str, Any]:
        """Per-node health snapshot for /dbstatus."""
        report: List[Dict[str, Any]] = []
        for node in self.nodes:
            reachable = await node.check()
            size = await node.data_size() if reachable else 0
            doc_count = 0
            if reachable and node.collection is not None:
                try:
                    doc_count = await node.collection.estimated_document_count()
                except Exception:
                    doc_count = 0
            report.append(
                {
                    "index": node.index,
                    "label": node.label,
                    "reachable": reachable,
                    "active": node.index == self.active_index,
                    "size": size,
                    "limit": self.max_bytes,
                    "full": node.is_full(size, self.max_bytes),
                    "documents": doc_count,
                }
            )
        return {
            "nodes": report,
            "active_index": self.active_index,
            "switches": self._switch_count,
            "last_switch_reason": self.last_switch_reason,
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def build_manager_from_env() -> MultiDatabaseManager:
    """Construct the manager from info.py values + optional env overrides."""
    from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME

    return MultiDatabaseManager(
        primary_uri=DATABASE_URI,
        primary_db=DATABASE_NAME,
        primary_collection=COLLECTION_NAME,
        secondary_uris=_env_list("SECONDARY_DATABASE_URIS"),
        secondary_dbs=_env_list("SECONDARY_DATABASE_NAMES"),
        secondary_collection=(_env_list("SECONDARY_COLLECTION_NAME") or [None])[0],
        max_bytes=_env_int("DATABASE_MAX_BYTES", 0),
    )


manager: Optional[MultiDatabaseManager] = None


def get_manager() -> MultiDatabaseManager:
    """Return the global manager, constructing it on first call (no I/O here)."""
    global manager
    if manager is None:
        manager = build_manager_from_env()
    return manager
