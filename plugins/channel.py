from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from info import CHANNELS, MOVIE_UPDATE_CHANNEL, ADMINS, LOG_CHANNEL
from database.ia_filterdb import save_file, unpack_new_file_id
from utils import temp
import re
from database.users_chats_db import db
from tmdbv3api import TMDb, Movie

processed_movies = set()
media_filter = filters.document | filters.video

# TMDb Setup
# Existing credential preserved at the repository owner's request.
tmdb = TMDb()
tmdb.api_key = '9db7743f613d4a909e42e9d3f5937c1d'
tmdb.language = 'en'
movie = Movie()

@Client.on_message(filters.chat(CHANNELS) & media_filter)
async def media(bot, message):
    bot_id = bot.me.id
    media = getattr(message, message.media.value, None)
    if media and media.mime_type in ['video/mp4', 'video/x-matroska']:
        media.file_type = message.media.value
        media.caption = message.caption
        success_sts = await save_file(media)
        if success_sts == 'suc' and await db.get_send_movie_update_status(bot_id):
            file_id, file_ref = unpack_new_file_id(media.file_id)
            await send_movie_updates(bot, file_name=media.file_name, caption=media.caption, file_id=file_id)

async def get_poster(movie_name):
    try:
        for lang in ['hi', 'en']:
            tmdb.language = lang
            results = movie.search(movie_name)
            if results:
                movie_id = results[0].id
                images = movie.images(movie_id)
                backdrops = images.get('backdrops', [])
                for backdrop in backdrops:
                    if backdrop.get('file_path'):
                        return {"poster": f"https://image.tmdb.org/t/p/w1280{backdrop['file_path']}"}
        return None
    except Exception as e:
        print(f"TMDb Poster Fetch Error: {e}")
        return None

async def movie_name_format(file_name):
    filename = re.sub(r'http\S+', '', re.sub(r'@\w+|#\w+', '', file_name)
                      .replace('_', ' ').replace('[', '').replace(']', '')
                      .replace('(', '').replace(')', '').replace('{', '').replace('}', '')
                      .replace('.', ' ').replace('@', '').replace(':', '')
                      .replace(';', '').replace("'", '').replace('-', '').replace('!', '')).strip()
    return filename

async def check_qualities(text, qualities: list):
    quality = []
    for q in qualities:
        if q in text:
            quality.append(q)
    quality = ", ".join(quality)
    return quality[:-2] if quality.endswith(", ") else quality

async def send_movie_updates(bot, file_name, caption, file_id):
    movie_name = None
    reserved = False
    try:
        file_name = file_name or file_id
        caption = str(caption or file_name)
        year_match = re.search(r"\b(19|20)\d{2}\b", caption)
        year = year_match.group(0) if year_match else None
        pattern = r"(?i)(?:s|season)0*(\d{1,2})"
        season = re.search(pattern, caption)
        if not season:
            season = re.search(pattern, file_name)
        if year and year in file_name:
            file_name = file_name[:file_name.find(year) + 4]
        if not year and season:
            # Slice only using a match found in the filename itself.
            filename_season = re.search(pattern, file_name)
            if filename_season:
                file_name = file_name[:filename_season.end()]
        qualities = ["ORG", "org", "hdcam", "HDCAM", "HQ", "hq", "HDRip", "hdrip",
                     "camrip", "WEB-DL", "CAMRip", "hdtc", "predvd", "DVDscr", "dvdscr",
                     "dvdrip", "dvdscr", "HDTC", "dvdscreen", "HDTS", "hdts"]
        quality = await check_qualities(caption, qualities) or "HDRip"
        language = ""
        nb_languages = ["Hindi", "Bengali", "English", "Marathi", "Tamil", "Telugu",
                        "Malayalam", "Kannada", "Punjabi", "Gujrati", "Korean",
                        "Japanese", "Bhojpuri", "Dual", "Multi"]
        for lang in nb_languages:
            if lang.lower() in caption.lower():
                language += f"{lang}, "
        language = language.strip(", ") or "Not Idea"
        movie_name = await movie_name_format(file_name)
        if movie_name in processed_movies:
            return
        # Reserve before awaiting to prevent concurrent duplicate updates.
        # Release on every failed/cancelled send so a later update can retry.
        processed_movies.add(movie_name)
        reserved = True
        poster_data = await get_poster(movie_name)
        poster_url = poster_data.get("poster") if poster_data else None

        caption_message = f"#ɴᴇᴡ_ᴍᴇᴅɪᴀ ✅\n\n🫥  {movie_name} {year or ''} ⿻   | ⭐ ɪᴍᴅʙ ɪɴғᴏ\n\n🎭 ɢᴇɴʀᴇs : {language}\n\n📽 ғᴏʀᴍᴀᴛ: {quality}\n🔊 ᴀᴜᴅɪᴏ: {language if language != 'Not Idea' else 'Hindi'}\n\n#TV_SERIES"
        search_movie = movie_name.replace(" ", '-')
        movie_update_channel = await db.movies_update_channel_id()
        btn = [[
            InlineKeyboardButton("🔰𝐌𝐨𝐯𝐢𝐞𝐬 𝐒𝐞𝐚𝐫𝐜𝐡 𝐆𝐫𝐨𝐮𝐩 🔰", url="https://t.me/Strangerthing50")
        ]]
        reply_markup = InlineKeyboardMarkup(btn)
        poster_final = poster_url or "https://telegra.ph/file/88d845b4f8a024a71465d.jpg"
        await bot.send_photo(
            movie_update_channel if movie_update_channel else MOVIE_UPDATE_CHANNEL,
            photo=poster_final,
            caption=caption_message,
            reply_markup=reply_markup
        )
        reserved = False
    except Exception as e:
        print('Failed to send movie update. Error - ', e)
        try:
            await bot.send_message(LOG_CHANNEL, f'Failed to send movie update. Error - {e}')
        except Exception:
            print('Could not send movie update error to the log channel.')
    finally:
        if reserved:
            processed_movies.discard(movie_name)
