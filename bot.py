
from pyrogram import idle
import logging
import logging.config

# Get logging configurations
logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)


from pyrogram import Client, __version__
from pyrogram.raw.all import layer
from database.ia_filterdb import ensure_all_indexes
from database.users_chats_db import db
from info import *
from utils import temp
from typing import Union, Optional, AsyncGenerator
from pyrogram import types
from Script import script 
from datetime import date, datetime 
import pytz
from aiohttp import web
from plugins import web_server

import asyncio
from pyrogram import idle
from Jisshu.bot import JisshuBot
from Jisshu.util.keepalive import ping_server
from Jisshu.bot.clients import initialize_clients

# Speedups: wzgram is backed by WarpCrypto (Rust) for MTProto encryption, and the
# `wzgram[fast]` extra also brings uvloop. The policy has to be installed before
# the event loop is created, and uvloop is unavailable on Windows/unsupported
# platforms, so a missing extra must never stop the bot from starting.
try:
    import uvloop

    uvloop.install()
    logging.info("uvloop event loop policy enabled (wzgram[fast] speedups)")
except ImportError:
    logging.info("uvloop not installed; using the default asyncio event loop")

loop = asyncio.get_event_loop()


def _notify_db_switch(previous, new, reason):
    """Log-channel notification fired by the manager after every DB switch."""
    try:
        loop.create_task(
            JisshuBot.send_message(
                LOG_CHANNEL,
                text=(
                    "<b>#DatabaseSwitch</b>\n\n"
                    f"`{previous.label}` ➜ `{new.label}`\n"
                    f"<b>Reason:</b> {reason}"
                ),
            )
        )
    except Exception:
        logging.exception("Failed to queue DB switch notification")


async def Jisshu_start():
    print('\n')
    print('Initalizing The Movie Provider Bot')
    # Pyrogram imports every module below plugins/ (plugins/Extra, plugins/helper
    # included) and registers their handlers the moment the client starts, so
    # the plugins must NOT be imported a second time by hand here: a manual
    # re-execution would replace the modules in sys.modules while the dispatcher
    # keeps running the first copy.
    await JisshuBot.start()
    bot_info = await JisshuBot.get_me()
    JisshuBot.username = bot_info.username
    await initialize_clients()
    if ON_HEROKU:
        asyncio.create_task(ping_server())
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats
    # Multi-database manager: connect every configured node before first use.
    from database.db_manager import get_manager
    db_manager = get_manager()
    db_manager.add_switch_listener(_notify_db_switch)
    await db_manager.initialize()
    await ensure_all_indexes()
    me = await JisshuBot.get_me()
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    JisshuBot.username = '@' + me.username
    logging.info(f"{me.first_name} with for Pyrogram v{__version__} (Layer {layer}) started on {me.username}.")
    logging.info(script.LOGO)
    tz = pytz.timezone('Asia/Kolkata')
    today = date.today()
    now = datetime.now(tz)
    time = now.strftime("%H:%M:%S %p")
    await JisshuBot.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_TXT.format(today, time))
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()
    await idle()


if __name__ == '__main__':
    try:
        loop.run_until_complete(Jisshu_start())
    except KeyboardInterrupt:
        logging.info('Service Stopped Bye 👋')
