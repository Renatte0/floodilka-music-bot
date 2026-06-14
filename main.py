import asyncio
import os
import sys
import logging

# Переходим в папку бота чтобы .env и импорты всегда находились
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot import FloodilkaBot
from music_player import MusicPlayer, resolve_track, _fmt_duration

logger = logging.getLogger(__name__)

TOKEN = os.environ.get("FLOODILKA_BOT_TOKEN")
if not TOKEN:
    raise EnvironmentError("Создайте файл .env и укажите FLOODILKA_BOT_TOKEN=<токен>")

bot = FloodilkaBot(TOKEN)

# per-guild music players
_players: dict[str, MusicPlayer] = {}


async def get_or_create_player(guild_id: str, channel_id: str) -> MusicPlayer | None:
    """Connect to voice if needed and return the guild's MusicPlayer."""
    player = _players.get(guild_id)
    if player and player.connected:
        return player

    try:
        voice_data = await bot.join_voice(guild_id, channel_id)
    except asyncio.TimeoutError:
        return None

    livekit_url: str = voice_data.get("endpoint") or voice_data.get("url") or ""
    livekit_token: str = voice_data.get("token") or ""

    if not livekit_url or not livekit_token:
        logger.error("VOICE_SERVER_UPDATE missing endpoint/token: %s", voice_data)
        return None

    # LiveKit требует wss:// или ws://, конвертируем https:// если нужно
    livekit_url = livekit_url.replace("https://", "wss://").replace("http://", "ws://")
    logger.info("Подключаюсь к LiveKit: %s", livekit_url)

    try:
        player = MusicPlayer()
        await player.connect(livekit_url, livekit_token)
        _players[guild_id] = player
        logger.info("LiveKit подключён успешно")
        return player
    except Exception:
        logger.exception("Ошибка подключения к LiveKit")
        return None


async def disconnect_player(guild_id: str) -> None:
    player = _players.pop(guild_id, None)
    if player:
        await player.disconnect()
    await bot.leave_voice(guild_id)


# ── Event handlers ──────────────────────────────────────────────────────────

@bot.on("READY")
async def on_ready(data: dict):
    user = data.get("user", {})
    print(f"Бот запущен: {user.get('username')} (id={user.get('id')})")
    print("Команды: !play <запрос/ссылка> | !pause | !resume | !skip | !stop | !queue | !np")


@bot.on("MESSAGE_CREATE")
async def on_message(data: dict):
    author = data.get("author", {})
    if author.get("bot"):
        return

    content: str = (data.get("content") or "").strip()
    channel_id: str = data.get("channel_id", "")
    guild_id: str = data.get("guild_id", "")
    user_id: str = author.get("id", "")

    if not content.startswith("!"):
        return

    parts = content.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    # ── !play ──────────────────────────────────────────────────────────────
    if cmd == "!play":
        if not arg:
            await bot.send_message(channel_id, "Использование: `!play <название или ссылка>`")
            return

        voice_channel = bot.get_user_voice_channel(guild_id, user_id)
        if not voice_channel:
            await bot.send_message(channel_id, "Сначала зайди в голосовой канал.")
            return

        searching_msg = await bot.send_message(channel_id, f"Ищу: **{arg}**...")

        try:
            track = await resolve_track(arg, requested_by=author.get("username", "?"))
        except Exception as e:
            await bot.edit_message(channel_id, searching_msg["id"], f"Ошибка поиска: {e}")
            return

        player = await get_or_create_player(guild_id, voice_channel)
        if not player:
            await bot.edit_message(channel_id, searching_msg["id"],
                                   "Не удалось подключиться к голосовому каналу.")
            return

        player.enqueue(track)
        started = await player.play()

        duration_str = _fmt_duration(track.duration) if track.duration else "?"
        if started:
            await bot.edit_message(channel_id, searching_msg["id"],
                                   f"Играет: **{track.title}** [{duration_str}]")
        else:
            pos = len(player.queue)
            await bot.edit_message(channel_id, searching_msg["id"],
                                   f"Добавлено в очередь [#{pos}]: **{track.title}** [{duration_str}]")

    # ── !pause ─────────────────────────────────────────────────────────────
    elif cmd == "!pause":
        player = _players.get(guild_id)
        if player and player.is_playing:
            player.pause()
            await bot.send_message(channel_id, "Пауза.")
        else:
            await bot.send_message(channel_id, "Ничего не играет.")

    # ── !resume ────────────────────────────────────────────────────────────
    elif cmd == "!resume":
        player = _players.get(guild_id)
        if player and player.is_paused:
            player.resume()
            await bot.send_message(channel_id, "Продолжаю.")
        else:
            await bot.send_message(channel_id, "Ничего не на паузе.")

    # ── !skip ──────────────────────────────────────────────────────────────
    elif cmd == "!skip":
        player = _players.get(guild_id)
        if player and player.current:
            skipped = player.current.title
            await player.skip()
            nxt = player.current
            if nxt:
                await bot.send_message(channel_id,
                                       f"Пропущено: **{skipped}**\nТеперь играет: **{nxt.title}**")
            else:
                await bot.send_message(channel_id, f"Пропущено: **{skipped}**. Очередь пуста.")
        else:
            await bot.send_message(channel_id, "Ничего не играет.")

    # ── !stop ──────────────────────────────────────────────────────────────
    elif cmd == "!stop":
        player = _players.get(guild_id)
        if player:
            await player.stop()
            await disconnect_player(guild_id)
            await bot.send_message(channel_id, "Остановлено. Вышел из голосового канала.")
        else:
            await bot.send_message(channel_id, "Бот не в голосовом канале.")

    # ── !np ────────────────────────────────────────────────────────────────
    elif cmd in ("!np", "!nowplaying"):
        player = _players.get(guild_id)
        if player and player.current:
            t = player.current
            status = "⏸ Пауза" if player.is_paused else "▶ Играет"
            duration_str = _fmt_duration(t.duration) if t.duration else "?"
            await bot.send_message(
                channel_id,
                f"{status}: **{t.title}** [{duration_str}]\n"
                f"Запрошено: {t.requested_by} | {t.webpage_url}"
            )
        else:
            await bot.send_message(channel_id, "Сейчас ничего не играет.")

    # ── !queue ─────────────────────────────────────────────────────────────
    elif cmd in ("!queue", "!q"):
        player = _players.get(guild_id)
        if not player or (not player.current and not player.queue):
            await bot.send_message(channel_id, "Очередь пуста.")
            return

        lines = []
        if player.current:
            status = "⏸" if player.is_paused else "▶"
            d = _fmt_duration(player.current.duration) if player.current.duration else "?"
            lines.append(f"{status} **{player.current.title}** [{d}]")

        for i, t in enumerate(player.queue, 1):
            d = _fmt_duration(t.duration) if t.duration else "?"
            lines.append(f"{i}. {t.title} [{d}] — {t.requested_by}")

        await bot.send_message(channel_id, "\n".join(lines))


if __name__ == "__main__":
    bot.start()
