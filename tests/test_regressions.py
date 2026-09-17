"""Offline characterization and regression tests; never load production credentials.

Load selected function definitions rather than importing modules which create
MongoDB/Telegram clients at import time. These tests do not replace integration
checks against an isolated Telegram bot and database.
"""
import ast
import asyncio
import logging
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

ROOT = Path(__file__).resolve().parents[1]


def load_functions(path, names, namespace):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    selected = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            node.decorator_list = []
            selected.append(node)
    if len(selected) != len(names):
        raise AssertionError(f"Missing functions in {path}")
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, str(ROOT / path), "exec"), namespace)
    return namespace


class FloodWait(Exception):
    def __init__(self, value):
        self.value = value


class InputUserDeactivated(Exception):
    pass


class UserIsBlocked(Exception):
    pass


class PeerIdInvalid(Exception):
    pass


class BroadcastTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sleep = AsyncMock()
        self.db = SimpleNamespace(delete_user=AsyncMock(), delete_chat=AsyncMock())
        self.ns = load_functions("utils.py", {"users_broadcast", "groups_broadcast"}, {
            "asyncio": SimpleNamespace(sleep=self.sleep), "db": self.db,
            "logging": logging, "logger": logging.getLogger(__name__),
            "FloodWait": FloodWait, "InputUserDeactivated": InputUserDeactivated,
            "UserIsBlocked": UserIsBlocked, "PeerIdInvalid": PeerIdInvalid,
        })
        self.sent = SimpleNamespace(pin=AsyncMock())
        self.message = SimpleNamespace(copy=AsyncMock(return_value=self.sent))

    async def test_user_success_contract_and_pin(self):
        result = await self.ns["users_broadcast"](42, self.message, True)
        self.assertEqual(result, (True, "Success"))
        self.sent.pin.assert_awaited_once_with(both_sides=True)

    async def test_group_success_contract(self):
        self.assertEqual(await self.ns["groups_broadcast"](-42, self.message, False), "Success")
        self.sent.pin.assert_not_awaited()

    async def test_user_flood_wait_preserves_pin(self):
        self.message.copy.side_effect = [FloodWait(2), self.sent]
        self.assertEqual(await self.ns["users_broadcast"](42, self.message, True), (True, "Success"))
        self.sleep.assert_awaited_once_with(2)
        self.sent.pin.assert_awaited_once_with(both_sides=True)

    async def test_group_flood_wait_preserves_pin(self):
        self.message.copy.side_effect = [FloodWait(3), self.sent]
        self.assertEqual(await self.ns["groups_broadcast"](-42, self.message, True), "Success")
        self.sleep.assert_awaited_once_with(3)
        self.sent.pin.assert_awaited_once_with()

    async def test_pin_failure_does_not_repeat_delivery(self):
        self.sent.pin.side_effect = FloodWait(1)
        self.assertEqual(await self.ns["users_broadcast"](42, self.message, True), (True, "Success"))
        self.message.copy.assert_awaited_once()

    async def test_transient_group_error_keeps_database_record(self):
        self.message.copy.side_effect = OSError("temporary transport failure")
        self.assertEqual(await self.ns["groups_broadcast"](-42, self.message, False), "Error")
        self.db.delete_chat.assert_not_awaited()

    async def test_blocked_user_contract(self):
        self.message.copy.side_effect = UserIsBlocked()
        self.assertEqual(await self.ns["users_broadcast"](42, self.message, False), (False, "Blocked"))
        self.db.delete_user.assert_awaited_once_with(42)


async def async_rows(rows):
    for row in rows:
        yield row


class BroadcastCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.status = SimpleNamespace(edit=AsyncMock())
        answer = SimpleNamespace(text="No", delete=AsyncMock())
        self.message = SimpleNamespace(
            ask=AsyncMock(return_value=answer), reply_to_message=object(),
            reply_text=AsyncMock(return_value=self.status),
            reply=AsyncMock(return_value=self.status),
        )
        self.db = SimpleNamespace(
            get_all_users=AsyncMock(return_value=async_rows([{"id": 42}])),
            total_users_count=AsyncMock(return_value=1),
            get_all_chats=AsyncMock(return_value=async_rows([])),
            total_chat_count=AsyncMock(return_value=0),
        )
        self.ns = load_functions("plugins/broadcast.py", {"broadcast_users", "broadcast_group", "broadcast_cancel"}, {
            "db": self.db, "lock": asyncio.Lock(), "ADMINS": [42],
            "temp": SimpleNamespace(USERS_CANCEL=False, GROUPS_CANCEL=False),
            "users_broadcast": AsyncMock(return_value=(True, "Success")),
            "groups_broadcast": AsyncMock(return_value="Success"),
            "get_readable_time": lambda seconds: "0sec",
            "time": SimpleNamespace(time=lambda: 0), "logger": logging.getLogger(__name__),
            "ReplyKeyboardMarkup": Mock(), "InlineKeyboardButton": Mock(),
            "InlineKeyboardMarkup": Mock(),
        })

    async def test_user_success_is_counted(self):
        await self.ns["broadcast_users"](None, self.message)
        final = self.status.edit.call_args.args[0]
        self.assertIn("Success: <code>1</code>", final)

    async def test_empty_group_broadcast_finishes(self):
        await self.ns["broadcast_group"](None, self.message)
        self.assertIn("completed", self.status.edit.call_args.args[0].lower())
        self.assertIn("<code>0 / 0</code>", self.status.edit.call_args.args[0])

    async def test_non_admin_cannot_cancel(self):
        query = SimpleNamespace(from_user=SimpleNamespace(id=99, username="outsider"),
                                data="broadcast_cancel#users", message=self.status, answer=AsyncMock())
        await self.ns["broadcast_cancel"](None, query)
        self.assertFalse(self.ns["temp"].USERS_CANCEL)
        self.status.edit.assert_not_awaited()


class Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.offset = 0
        self.maximum = len(rows)

    def sort(self, *args):
        return self

    def skip(self, value):
        self.offset = value
        return self

    def limit(self, value):
        self.maximum = value
        return self

    async def to_list(self, length):
        return self.rows[self.offset:self.offset + min(length, self.maximum)]

    def __aiter__(self):
        return async_rows(self.rows)


class SearchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.filters = []
        self.rows = [SimpleNamespace(file_name="Movie Hindi 2024"), SimpleNamespace(file_name="Movie English 2024")]
        def find(query):
            self.filters.append(query)
            return Cursor(self.rows)
        self.ns = load_functions("database/ia_filterdb.py", {"get_search_results"}, {
            "re": re, "MAX_BTN": 8,
            "Media": SimpleNamespace(find=find, count_documents=AsyncMock(return_value=2)),
        })

    async def test_pagination_contract(self):
        files, next_offset, count = await self.ns["get_search_results"]("Movie", max_results=1)
        self.assertEqual((len(files), next_offset, count), (1, 1, 2))
        _, next_offset, count = await self.ns["get_search_results"]("Movie", max_results=1, offset=1)
        self.assertEqual((next_offset, count), ("", 2))

    async def test_regex_metacharacters_are_literal(self):
        await self.ns["get_search_results"]("Movie.*")
        pattern = self.filters[-1]["file_name"]
        self.assertIsNone(pattern.search("MovieAnything"))
        self.assertIsNotNone(pattern.search("Movie.* 2024"))

    async def test_unbalanced_bracket_is_searchable(self):
        await self.ns["get_search_results"]("Movie[")
        self.assertIsInstance(self.filters[-1]["file_name"], re.Pattern)
        self.assertIsNotNone(self.filters[-1]["file_name"].search("Movie[ 2024"))

    async def test_multiword_matching_keeps_separator_support(self):
        await self.ns["get_search_results"]("Movie 2024")
        self.assertIsNotNone(self.filters[-1]["file_name"].search("Movie Hindi 2024"))

    async def test_language_filter_is_case_insensitive(self):
        files, _, count = await self.ns["get_search_results"]("Movie", lang="HINDI")
        self.assertEqual((len(files), count), (1, 1))


class SaveTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_mime_type_is_allowed(self):
        document = SimpleNamespace(commit=AsyncMock())
        factory = Mock(return_value=document)
        ns = load_functions("database/ia_filterdb.py", {"save_file"}, {
            "re": re, "Media": factory,
            "unpack_new_file_id": lambda value: ("id", "ref"),
            "ValidationError": type("ValidationError", (Exception,), {}),
            "DuplicateKeyError": type("DuplicateKeyError", (Exception,), {}),
        })
        media = SimpleNamespace(file_id="id", file_name="Movie.mp4", file_size=42, mime_type=None, caption=None)
        self.assertEqual(await ns["save_file"](media), "suc")
        document.commit.assert_awaited_once()
        self.assertIsNone(factory.call_args.kwargs["file_type"])


class MovieUpdateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ns = load_functions("plugins/channel.py", {"send_movie_updates", "movie_name_format", "check_qualities"}, {
            "re": re, "processed_movies": set(),
            "db": SimpleNamespace(movies_update_channel_id=AsyncMock(return_value=None)),
            "get_poster": AsyncMock(return_value=None),
            "InlineKeyboardButton": Mock(), "InlineKeyboardMarkup": Mock(),
            "MOVIE_UPDATE_CHANNEL": -1, "LOG_CHANNEL": -2,
        })
        self.bot = SimpleNamespace(send_photo=AsyncMock(), send_message=AsyncMock())

    async def test_captionless_update(self):
        await self.ns["send_movie_updates"](self.bot, "Movie 2024.mp4", None, "id")
        self.bot.send_photo.assert_awaited_once()

    async def test_failed_send_can_be_retried(self):
        self.bot.send_photo.side_effect = [OSError("temporary"), None]
        for _ in range(2):
            await self.ns["send_movie_updates"](self.bot, "Movie 2024.mp4", "Movie 2024", "id")
        self.assertEqual(self.bot.send_photo.await_count, 2)

    async def test_successful_update_is_not_sent_twice(self):
        for _ in range(2):
            await self.ns["send_movie_updates"](self.bot, "Movie 2024.mp4", "Movie 2024", "id")
        self.bot.send_photo.assert_awaited_once()

    async def test_logging_failure_does_not_escape(self):
        self.bot.send_photo.side_effect = OSError("temporary")
        self.bot.send_message.side_effect = OSError("log unavailable")
        await self.ns["send_movie_updates"](self.bot, "Movie 2024.mp4", "Movie 2024", "id")
        self.assertFalse(self.ns["processed_movies"])


class DependencyTests(unittest.TestCase):
    def test_single_pyrogram_provider(self):
        lines = (ROOT / "requirements.txt").read_text().lower().splitlines()
        providers = [line.strip() for line in lines if line.strip().startswith(("pyrogram", "pyrofork"))]
        self.assertEqual(len(providers), 1, providers)


if __name__ == "__main__":
    unittest.main()
