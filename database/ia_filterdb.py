"""
Media store built on top of the multi-database manager.

Reads fall back across every configured database node, so previously written
data stays reachable no matter which node holds it. Writes go to the active
node; when the active node is full or unavailable, the manager switches nodes
automatically and the write is retried once on the new node.

Public surface (call-signatures unchanged, plugins need no edits):
  Media                      - umongo Document bound to the ACTIVE node
  save_file(media)           - 'suc' / 'dup' / 'err'
  get_search_results(...)    - (files, next_offset, total_results)
  get_bad_files(...)         - (files, total_results)
  get_file_details(file_id)  - [document]
  get_files_db_size()        - active node's dataSize (int)
  db_size_bytes()            - total bytes across all configured nodes
  node_stats()               - per-node info for /dbstatus
"""
import asyncio
import logging
import base64
from struct import pack
import re

from pyrogram.file_id import FileId
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from marshmallow.exceptions import ValidationError
from info import COLLECTION_NAME, MAX_BTN
from database.db_manager import get_manager, AllDatabasesUnavailable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Node:
    """Lazy umongo Instance + Document facade for one MongoDB node."""

    def __init__(self, index, uri, db_name, collection_name):
        self.index = index
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.label = f"DB-{index + 1}"
        self.client = None
        self.mydb = None
        self.instance = None
        self.Media = None

    def connect(self, client=None, db=None):
        if self.client is not None:
            return True
        try:
            if client is None:
                client = AsyncIOMotorClient(
                    self.uri,
                    serverSelectionTimeoutMS=10000,
                    connectTimeoutMS=10000,
                    retryWrites=True,
                )
            self.client = client
            self.mydb = db if db is not None else client[self.db_name]
            self.instance = Instance.from_db(self.mydb)

            @self.instance.register
            class Media(Document):
                file_id = fields.StrField(attribute='_id')
                file_ref = fields.StrField(allow_none=True)
                file_name = fields.StrField(required=True)
                file_size = fields.IntField(required=True)
                mime_type = fields.StrField(allow_none=True)
                caption = fields.StrField(allow_none=True)
                file_type = fields.StrField(allow_none=True)

                class Meta:
                    indexes = ('$file_name',)
                    collection_name = self.collection_name

            self.Media = Media
            logger.info("Node %s configured (%s/%s)", self.label, self.db_name, self.collection_name)
            return True
        except Exception as exc:
            logger.error("Node %s configuration error: %s", self.label, exc)
            self.client = None
            self.mydb = None
            self.instance = None
            self.Media = None
            return False

    async def check(self):
        if self.client is None:
            return False
        try:
            await asyncio.wait_for(self.client.admin.command("ping"), timeout=10)
            return True
        except Exception as exc:
            logger.warning("Node %s failed connectivity check: %s", self.label, exc)
            return False


# ---------------------------------------------------------------------------
# Media-layer operations
# ---------------------------------------------------------------------------

_manager = None
_nodes: list = []
_ready = False
_ready_lock = asyncio.Lock()


async def _ensure():
    """Wire the media layer to every configured node (idempotent)."""
    global _manager, _ready
    if _ready:
        return
    async with _ready_lock:
        if _ready:
            return
        _manager = get_manager()
        await _manager.initialize()
        for mnode in _manager.nodes:
            node = Node(mnode.index, mnode.uri, mnode.db_name, mnode.collection_name)
            # Reuse the manager's client so every layer shares one pool per node.
            if node.connect(client=mnode.client, db=mnode.db):
                _nodes.append(node)
            else:
                logger.error("Skipping media node %s (configuration failed)", node.label)
        if not _nodes:
            raise AllDatabasesUnavailable("No media node could be configured")
        _ready = True
        logger.info("Media layer ready across %d node(s)", len(_nodes))


def _active_node():
    """The local Node matching the manager's active node (matched by identity,
    not list index, so a skipped node can never shift the mapping)."""
    if not _ready:
        raise AllDatabasesUnavailable("Media layer is not ready")
    m = _manager.active
    for node in _nodes:
        if (node.uri, node.db_name, node.collection_name) == (
            m.uri, m.db_name, m.collection_name,
        ):
            return node
    raise AllDatabasesUnavailable(f"Active node {m.label} has no media mapping")


def _nodes_in_read_order():
    """Active node first, remaining nodes in configured order."""
    try:
        active = _active_node()
    except AllDatabasesUnavailable:
        return list(_nodes)
    return [active] + [n for n in _nodes if n is not active]


class _ActiveMediaFacade:
    """Proxies the active node's umongo Media class.

    Class-level operations (find, find_one, count_documents, collection, ...)
    behave like the original single-database Media class, with two upgrades:
    find_one searches every node and deletions broadcast to every node so
    cross-node reads cannot resurface deleted files.
    """

    def __getattr__(self, name):
        if name == "collection":
            return _BroadcastingCollection()
        return getattr(_active_node().Media, name)

    async def count_documents(self, filter_=None, **kwargs):
        """Count matching documents across EVERY node, not just the active one.

        /stats, /deleteall and the "delete all files" callback all report this
        number, so it must cover files that live on a secondary node.
        """
        await _ensure()
        total = 0
        for node in _nodes:
            try:
                total += await node.Media.count_documents(filter_ or {}, **kwargs)
            except Exception as exc:
                logger.warning("count_documents on %s failed: %s", node.label, exc)
        return total

    async def find_one(self, *args, **kwargs):
        """Search every node; returns the umongo document bound to its node
        (so document.delete() keeps working on the right node)."""
        await _ensure()
        for node in _nodes_in_read_order():
            try:
                doc = await node.Media.find_one(*args, **kwargs)
            except Exception as exc:
                logger.warning("find_one on %s failed: %s", node.label, exc)
                continue
            if doc is not None:
                return doc
        return None

    async def delete_many(self, filter_):
        await _ensure()
        deleted = 0
        for node in _nodes:
            try:
                res = await node.Media.collection.delete_many(filter_)
                deleted += res.deleted_count or 0
            except Exception as exc:
                logger.warning("delete_many on %s failed: %s", node.label, exc)
        return _DeleteResult(deleted)

    async def delete_one(self, filter_):
        await _ensure()
        deleted = 0
        for node in _nodes:
            try:
                res = await node.Media.collection.delete_one(filter_)
                deleted += res.deleted_count or 0
            except Exception as exc:
                logger.warning("delete_one on %s failed: %s", node.label, exc)
        return _DeleteResult(deleted)


class _DeleteResult:
    __slots__ = ("deleted_count",)

    def __init__(self, deleted_count):
        self.deleted_count = deleted_count


class _BroadcastingCollection:
    """Raw-collection view whose destructive ops hit EVERY node.

    Plugins call Media.collection.delete_one/delete_many/drop; if those only
    touched the active node, files on other nodes would resurface in search.
    Everything else (find, aggregate, ...) passes through to the active node.
    """

    def __getattr__(self, name):
        return getattr(_active_node().Media.collection, name)

    async def delete_one(self, filter_, *args, **kwargs):
        await _ensure()
        deleted = 0
        for node in _nodes:
            try:
                res = await node.Media.collection.delete_one(filter_, *args, **kwargs)
                deleted += res.deleted_count or 0
            except Exception as exc:
                logger.warning("collection.delete_one on %s failed: %s", node.label, exc)
        return _DeleteResult(deleted)

    async def delete_many(self, filter_, *args, **kwargs):
        await _ensure()
        deleted = 0
        for node in _nodes:
            try:
                res = await node.Media.collection.delete_many(filter_, *args, **kwargs)
                deleted += res.deleted_count or 0
            except Exception as exc:
                logger.warning("collection.delete_many on %s failed: %s", node.label, exc)
        return _DeleteResult(deleted)

    async def drop(self, *args, **kwargs):
        await _ensure()
        for node in _nodes:
            try:
                await node.Media.collection.drop(*args, **kwargs)
            except Exception as exc:
                logger.warning("collection.drop on %s failed: %s", node.label, exc)


Media = _ActiveMediaFacade()


async def ensure_all_indexes():
    """Create the media indexes on every node (replaces Media.ensure_indexes)."""
    await _ensure()
    for node in _nodes:
        try:
            await node.Media.ensure_indexes()
        except Exception as exc:
            logger.warning("ensure_indexes on %s failed: %s", node.label, exc)


# ---------------------------------------------------------------------------
# Public API - writes (single retry after automatic node switch)
# ---------------------------------------------------------------------------

async def save_file(media):
    """Save a file on the active node; auto-switch and retry once on failure.

    Returns 'suc', 'dup' or 'err' exactly like the original implementation.
    """
    await _ensure()
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
    # Captions arrive either as a pyrogram Text object (.html) from some flows
    # or as a plain str (index/channel handlers assign message.caption).
    raw_caption = getattr(media, "caption", None)
    caption = raw_caption.html if hasattr(raw_caption, "html") else raw_caption
    for attempt in (1, 2):
        if not await _manager.ensure_capacity_ok():
            logger.error("save_file aborted: no database node has capacity")
            return 'err'
        try:
            file = _active_node().Media(
                file_id=file_id,
                file_ref=file_ref,
                file_name=file_name,
                file_size=media.file_size,
                mime_type=media.mime_type,
                caption=caption,
                file_type=media.mime_type.split('/')[0],
            )
        except ValidationError as exc:
            logger.error("Error occurred while saving file in database: %s", exc)
            return 'err'
        except Exception as exc:
            logger.error("save_file: could not build document: %s", exc)
            return 'err'
        try:
            await file.commit()
        except DuplicateKeyError:
            logger.info('%s is already saved in database', getattr(media, 'file_name', 'NO_FILE'))
            return 'dup'
        except Exception as exc:
            logger.warning(
                "save_file attempt %d on %s failed: %s", attempt, _active_node().label, exc,
            )
            switched = await _manager.handle_write_failure(exc)
            if not switched:
                logger.error("save_file failed and no alternative node is available")
                return 'err'
            continue
        logger.info('%s is saved to %s', getattr(media, 'file_name', 'NO_FILE'), _active_node().label)
        return 'suc'
    logger.error('save_file: write failed after failover retry')
    return 'err'


# ---------------------------------------------------------------------------
# Public API - reads (fall back across every node)
# ---------------------------------------------------------------------------

def _build_pattern(query: str):
    """Turn a user query into a regex, preserving original semantics."""
    query = query.strip()
    if not query:
        return '.'
    if ' ' not in query:
        return r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    return query.replace(' ', r'.*[\s\.\+\-_]')


async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    """Search across all nodes; pagination is global, not per node.

    Returns (files, next_offset, total_results) with the original contract:
    total_results counts every match on every node, so "next page" works and
    the result count shown to users is real.
    """
    await _ensure()
    pattern = _build_pattern(query)
    try:
        regex = re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        regex = query.strip() or '.'
    filter_ = {'file_name': regex}

    collected: list = []
    total_results = 0
    for node in _nodes_in_read_order():
        try:
            total_results += await node.Media.count_documents(filter_)
        except Exception as exc:
            logger.warning("count_documents on %s failed: %s", node.label, exc)
        try:
            cursor = node.Media.find(filter_)
            cursor.sort('$natural', -1)
            if lang:
                collected.extend(
                    [file async for file in cursor if lang in (file.file_name or '').lower()]
                )
            else:
                # Fetch a page + offset from each node so the global slice below
                # can be taken without loading whole collections.
                limit = max(1, offset + max_results)
                cursor.limit(limit)
                collected.extend(await cursor.to_list(length=limit))
        except Exception as exc:
            logger.warning("search on %s failed: %s", node.label, exc)

    if lang:
        # Language filtering happens in memory, so the match count is known here.
        total_results = len(collected)
    files = collected[offset:offset + max_results]
    next_offset = offset + max_results
    if next_offset >= total_results:
        next_offset = ''
    return files, next_offset, total_results


async def get_bad_files(query, file_type=None, offset=0, filter=False):
    """Every matching file across all nodes, plus the real match count.

    The delete flows (/killfiles, the killfilesak callback) iterate the returned
    list and show the total to the user, so returning only one page here would
    silently leave files behind on other nodes.
    """
    await _ensure()
    pattern = _build_pattern(query)
    try:
        regex = re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        return [], 0
    filter_ = {'file_name': regex}
    if file_type:
        filter_['file_type'] = file_type
    files: list = []
    total_results = 0
    for node in _nodes_in_read_order():
        try:
            node_total = await node.Media.count_documents(filter_)
        except Exception as exc:
            logger.warning("count_documents on %s failed: %s", node.label, exc)
            continue
        if not node_total:
            continue
        try:
            cursor = node.Media.find(filter_)
            cursor.sort('$natural', -1)
            node_files = await cursor.to_list(length=node_total)
        except Exception as exc:
            logger.warning("get_bad_files on %s failed: %s", node.label, exc)
            continue
        files.extend(node_files)
        total_results += node_total
    return files, total_results


async def get_file_details(query):
    """Look a file up on every node (first match wins)."""
    await _ensure()
    filter_ = {'file_id': query}
    for node in _nodes_in_read_order():
        try:
            cursor = node.Media.find(filter_)
            filedetails = await cursor.to_list(length=1)
        except Exception as exc:
            logger.warning("get_file_details on %s failed: %s", node.label, exc)
            continue
        if filedetails:
            return filedetails
    return []


async def total_media_count():
    """Estimated file count across every node (for /stats)."""
    await _ensure()
    total = 0
    for node in _nodes:
        try:
            total += await node.Media.count_documents({})
        except Exception as exc:
            logger.warning("count_documents on %s failed: %s", node.label, exc)
    return total


async def get_files_db_size():
    """Logical size (bytes) of the ACTIVE node's media database."""
    await _ensure()
    try:
        stats = await _active_node().mydb.command('dbstats')
        return int(stats.get('dataSize', 0) or 0)
    except Exception as exc:
        logger.warning("get_files_db_size failed on active node: %s", exc)
        return 0


async def db_size_bytes():
    """Total dataSize across every configured node."""
    await _ensure()
    total = 0
    for node in _nodes:
        try:
            stats = await node.mydb.command('dbstats')
            total += int(stats.get('dataSize', 0) or 0)
        except Exception as exc:
            logger.warning("db_size_bytes on %s failed: %s", node.label, exc)
    return total


async def node_stats():
    """Per-node snapshot for the /dbstatus admin command."""
    await _ensure()
    return await _manager.status_report()


# ---------------------------------------------------------------------------
# file_id codec (unchanged logic, moved verbatim from the original file)
# ---------------------------------------------------------------------------

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash,
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
