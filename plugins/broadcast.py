from pyrogram import Client, filters
import datetime
import time
from database.users_chats_db import db
from info import ADMINS
from utils import users_broadcast, groups_broadcast, temp, get_readable_time
import asyncio
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

lock = asyncio.Lock()

@Client.on_callback_query(filters.regex(r'^broadcast_cancel'))
async def broadcast_cancel(bot, query):
    user = query.from_user
    username = (getattr(user, 'username', None) or '').lower()
    if user.id not in ADMINS and not any(
        isinstance(admin, str) and username and admin.lstrip('@').lower() == username
        for admin in ADMINS
    ):
        return await query.answer('Only bot admins can cancel broadcasts.', show_alert=True)
    parts = query.data.split('#')
    if len(parts) != 2 or parts[1] not in ('users', 'groups'):
        return await query.answer('Invalid broadcast action.', show_alert=True)
    ident = parts[1]
    if ident == 'users':
        temp.USERS_CANCEL = True
        await query.message.edit("ᴛʀʏɪɴɢ ᴛᴏ ᴄᴀɴᴄᴇʟ ᴜsᴇʀs ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ...")
    elif ident == 'groups':
        temp.GROUPS_CANCEL = True
        await query.message.edit("ᴛʀʏɪɴɢ ᴛᴏ ᴄᴀɴᴄᴇʟ ɢʀᴏᴜᴘs ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ...")
    await query.answer()

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast_users(bot, message):
    try:
        if lock.locked():
            return await message.reply('Currently broadcast processing, wait for it to complete.')
        async with lock:
            temp.USERS_CANCEL = False
            msg = await message.ask(
                '<b>Do you want to pin this message to users?</b>',
                reply_markup=ReplyKeyboardMarkup([['Yes', 'No']], one_time_keyboard=True, resize_keyboard=True)
            )
            if msg.text == 'Yes':
                is_pin = True
            elif msg.text == 'No':
                is_pin = False
            else:
                return await message.reply('Wrong Response!')
            await msg.delete()
            users = await db.get_all_users()
            total_users = await db.total_users_count()
            b_msg = message.reply_to_message
            btn = [[InlineKeyboardButton('CANCEL', callback_data='broadcast_cancel#users')]]
            b_sts = await message.reply_text(
                '<b>Broadcasting your message to users ⌛️</b>',
                reply_markup=InlineKeyboardMarkup(btn)
            )
            start_time = time.time()
            done = success = failed = 0
            async for user in users:
                time_taken = get_readable_time(time.time() - start_time)
                if temp.USERS_CANCEL:
                    temp.USERS_CANCEL = False
                    await b_sts.edit(
                        f"Broadcast cancelled!\nCompleted in {time_taken}\n\n"
                        f"Total Users: <code>{total_users}</code>\n"
                        f"Completed: <code>{done} / {total_users}</code>\n"
                        f"Success: <code>{success}</code>\nFailed: <code>{failed}</code>"
                    )
                    return
                try:
                    delivered, status = await users_broadcast(int(user['id']), b_msg, is_pin)
                except Exception:
                    logger.exception("Failed to broadcast to user %s", user.get('id'))
                    delivered = False
                if delivered:
                    success += 1
                else:
                    failed += 1
                done += 1
                if done % 20 == 0:
                    try:
                        await b_sts.edit(
                            f"Broadcast in progress...\n\n"
                            f"Total Users: <code>{total_users}</code>\n"
                            f"Completed: <code>{done} / {total_users}</code>\n"
                            f"Success: <code>{success}</code>\nFailed: <code>{failed}</code>",
                            reply_markup=InlineKeyboardMarkup(btn)
                        )
                    except Exception:
                        logger.warning("Could not update user broadcast progress", exc_info=True)
            time_taken = get_readable_time(time.time() - start_time)
            await b_sts.edit(
                f"Broadcast completed.\nCompleted in {time_taken}\n\n"
                f"Total Users: <code>{total_users}</code>\n"
                f"Completed: <code>{done} / {total_users}</code>\n"
                f"Success: <code>{success}</code>\nFailed: <code>{failed}</code>"
            )
    except Exception as e:
        logger.exception("Error in broadcast command")
        await message.reply(f"An error occurred: {e}")

@Client.on_message(filters.command('grp_broadcast') & filters.user(ADMINS) & filters.reply)
async def broadcast_group(bot, message):
    if lock.locked():
        return await message.reply('Currently broadcast processing, wait for it to complete.')
    async with lock:
        temp.GROUPS_CANCEL = False
        msg = await message.ask('<b>Do you want pin this message in groups?</b>', reply_markup=ReplyKeyboardMarkup([['Yes', 'No']], one_time_keyboard=True, resize_keyboard=True))
        if msg.text == 'Yes':
            is_pin = True
        elif msg.text == 'No':
            is_pin = False
        else:
            return await message.reply('Wrong Response!')
        await msg.delete()
        chats = await db.get_all_chats()
        b_msg = message.reply_to_message
        btn = [[InlineKeyboardButton('CANCEL', callback_data='broadcast_cancel#groups')]]
        b_sts = await message.reply_text(
            text='<b>ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ʏᴏᴜʀ ᴍᴇssᴀɢᴇs ᴛᴏ ɢʀᴏᴜᴘs ⏳</b>',
            reply_markup=InlineKeyboardMarkup(btn)
        )
        start_time = time.time()
        total_chats = await db.total_chat_count()
        done = failed = success = 0
        async for chat in chats:
            time_taken = get_readable_time(time.time() - start_time)
            if temp.GROUPS_CANCEL:
                temp.GROUPS_CANCEL = False
                await b_sts.edit(f"Groups broadcast Cancelled!\nCompleted in {time_taken}\n\nTotal Groups: <code>{total_chats}</code>\nCompleted: <code>{done} / {total_chats}</code>\nSuccess: <code>{success}</code>\nFailed: <code>{failed}</code>")
                return
            try:
                sts = await groups_broadcast(int(chat['id']), b_msg, is_pin)
            except Exception:
                logger.exception("Failed to broadcast to group %s", chat.get('id'))
                sts = 'Error'
            if sts == 'Success':
                success += 1
            else:
                failed += 1
            done += 1
            if done % 20 == 0:
                try:
                    await b_sts.edit(f"Groups broadcast in progress...\n\nTotal Groups: <code>{total_chats}</code>\nCompleted: <code>{done} / {total_chats}</code>\nSuccess: <code>{success}</code>\nFailed: <code>{failed}</code>", reply_markup=InlineKeyboardMarkup(btn))
                except Exception:
                    logger.warning("Could not update group broadcast progress", exc_info=True)
        time_taken = get_readable_time(time.time() - start_time)
        await b_sts.edit(f"Groups broadcast completed.\nCompleted in {time_taken}\n\nTotal Groups: <code>{total_chats}</code>\nCompleted: <code>{done} / {total_chats}</code>\nSuccess: <code>{success}</code>\nFailed: <code>{failed}</code>")
