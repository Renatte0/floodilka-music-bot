import asyncio
import os
import sys
import logging
import time

# Переходим в папку бота чтобы .env и импорты всегда находились
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot import FloodilkaBot
from music_player import MusicPlayer, QueueFull, resolve_track, _fmt_duration

logger = logging.getLogger(__name__)

TOKEN = os.environ.get("FLOODILKA_BOT_TOKEN")
if not TOKEN:
    raise EnvironmentError("Создайте файл .env и укажите FLOODILKA_BOT_TOKEN=<токен>")

bot = FloodilkaBot(TOKEN)

IDLE_TIMEOUT = int(os.environ.get("IDLE_TIMEOUT", 30 * 60))
MAX_VOICE_SESSIONS = int(os.environ.get("MAX_VOICE_SESSIONS", "50"))
USER_RATE_LIMIT = int(os.environ.get("USER_RATE_LIMIT", "5"))
USER_RATE_WINDOW = float(os.environ.get("USER_RATE_WINDOW", "10"))

# per-guild music players
_players: dict[str, MusicPlayer] = {}
# сериализует join/leave и команды внутри одной гильдии
_guild_locks: dict[str, asyncio.Lock] = {}
# время последней активности (команды) по guild_id
_last_activity: dict[str, float] = {}
# channel_id для отправки сообщения об авто-выходе
_last_text_channel: dict[str, str] = {}
# {user_id: (window_start, count)} — простой счётчик команд
_user_hits: dict[str, tuple[float, int]] = {}


def _guild_lock(guild_id: str) -> asyncio.Lock:
    lock = _guild_locks.get(guild_id)
    if lock is None:
        lock = _guild_locks[guild_id] = asyncio.Lock()
    return lock


def _rate_limited(user_id: str) -> bool:
    now = time.monotonic()
    start, count = _user_hits.get(user_id, (now, 0))
    if now - start >= USER_RATE_WINDOW:
        _user_hits[user_id] = (now, 1)
        return False
    if count >= USER_RATE_LIMIT:
        return True
    _user_hits[user_id] = (start, count + 1)
    return False


async def get_or_create_player(guild_id: str, channel_id: str) -> MusicPlayer | None:
    """Connect to voice if needed and return the guild's MusicPlayer."""
    player = _players.get(guild_id)
    if player and player.connected:
        return player
    if player:
        # комната отвалилась — освобождаем слот перед повторным подключением
        await disconnect_player(guild_id)

    if len(_players) >= MAX_VOICE_SESSIONS:
        logger.warning("Voice session cap reached (%s)", MAX_VOICE_SESSIONS)
        return None

    try:
        voice_data = await bot.join_voice(guild_id, channel_id)
    except (asyncio.TimeoutError, asyncio.CancelledError, ConnectionError):
        return None

    livekit_url: str = voice_data.get("endpoint") or voice_data.get("url") or ""
    livekit_token: str = voice_data.get("token") or ""

    if not livekit_url or not livekit_token:
        logger.error("VOICE_SERVER_UPDATE missing endpoint/token for guild=%s", guild_id)
        return None

    # LiveKit требует wss:// или ws://, конвертируем https:// если нужно
    livekit_url = livekit_url.replace("https://", "wss://").replace("http://", "ws://")

    player = MusicPlayer()
    try:
        await player.connect(livekit_url, livekit_token)
    except Exception:
        logger.exception("Ошибка подключения к LiveKit guild=%s", guild_id)
        try:
            await player.disconnect()
        except Exception:
            pass
        await bot.leave_voice(guild_id)
        return None

    _players[guild_id] = player
    logger.info("LiveKit подключён (guild=%s, активных сессий=%s)", guild_id, len(_players))
    return player


async def disconnect_player(guild_id: str) -> None:
    player = _players.pop(guild_id, None)
    if player:
        try:
            await player.disconnect()
        except Exception:
            logger.exception("Ошибка при отключении плеера guild=%s", guild_id)
    _last_activity.pop(guild_id, None)
    _last_text_channel.pop(guild_id, None)
    try:
        await bot.leave_voice(guild_id)
    except Exception:
        pass


async def _idle_watcher() -> None:
    """Раз в минуту проверяет, не простаивает ли бот в канале дольше таймаута."""
    while True:
        await asyncio.sleep(60)
        try:
            now = time.monotonic()
            for guild_id in list(_players.keys()):
                player = _players.get(guild_id)
                if not player:
                    continue
                if not player.connected:
                    logger.info("Освобождаю мёртвую сессию guild=%s", guild_id)
                    async with _guild_lock(guild_id):
                        await disconnect_player(guild_id)
                    continue
                if player.is_playing:
                    _last_activity[guild_id] = now
                    continue
                last = _last_activity.get(guild_id, now)
                if now - last < IDLE_TIMEOUT:
                    continue

                logger.info("Авто-выход из канала (таймаут бездействия) guild=%s", guild_id)
                ch = _last_text_channel.get(guild_id)
                async with _guild_lock(guild_id):
                    await disconnect_player(guild_id)
                if ch:
                    try:
                        await bot.send_message(
                            ch, "Вышел из голосового канала из-за бездействия (30 мин)."
                        )
                    except Exception:
                        pass

            # чистим счётчики команд, иначе словарь растёт по числу юзеров
            cutoff = now - USER_RATE_WINDOW
            for uid in [u for u, (start, _) in _user_hits.items() if start < cutoff]:
                _user_hits.pop(uid, None)
            for gid in [g for g, lock in _guild_locks.items()
                        if g not in _players and not lock.locked()]:
                _guild_locks.pop(gid, None)
        except Exception:
            logger.exception("Ошибка в idle watcher")


# ── Event handlers ──────────────────────────────────────────────────────────

_idle_task: asyncio.Task | None = None


@bot.on("READY")
async def on_ready(data: dict):
    global _idle_task
    user = data.get("user", {})
    print(f"Бот запущен: {user.get('username')} (id={user.get('id')})")
    print("Команды: !play <запрос/ссылка> | !pause | !resume | !skip | !stop | !queue | !np")
    # READY прилетает заново на каждом реконнекте — без этой проверки
    # копии вотчера копились бы всё время работы бота
    if _idle_task is None or _idle_task.done():
        _idle_task = bot.spawn(_idle_watcher())


@bot.on("MESSAGE_CREATE")
async def on_message(data: dict):
    author = data.get("author", {})
    if author.get("bot"):
        return

    content: str = (data.get("content") or "").strip()
    if not content.startswith("!"):
        return

    channel_id: str = data.get("channel_id", "")
    guild_id: str = data.get("guild_id", "")
    user_id: str = author.get("id", "")
    if not guild_id:
        return

    parts = content.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    if cmd not in ("!play", "!pause", "!resume", "!skip", "!stop",
                   "!np", "!nowplaying", "!queue", "!q"):
        return

    if _rate_limited(user_id):
        return

    _last_activity[guild_id] = time.monotonic()
    _last_text_channel[guild_id] = channel_id

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

        # Без блокировки два одновременных !play подняли бы две LiveKit-сессии
        async with _guild_lock(guild_id):
            player = await get_or_create_player(guild_id, voice_channel)
            if not player:
                await bot.edit_message(channel_id, searching_msg["id"],
                                       "Не удалось подключиться к голосовому каналу.")
                return

            try:
                player.enqueue(track)
            except QueueFull as e:
                await bot.edit_message(channel_id, searching_msg["id"], str(e))
                return
            started = await player.play()
            pos = len(player.queue)

        duration_str = _fmt_duration(track.duration) if track.duration else "?"
        if started:
            await bot.edit_message(channel_id, searching_msg["id"],
                                   f"Играет: **{track.title}** [{duration_str}]")
        else:
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
        async with _guild_lock(guild_id):
            player = _players.get(guild_id)
            if not (player and player.current):
                await bot.send_message(channel_id, "Ничего не играет.")
                return
            skipped = player.current.title
            await player.skip()
            nxt = player.current

        if nxt:
            await bot.send_message(channel_id,
                                   f"Пропущено: **{skipped}**\nТеперь играет: **{nxt.title}**")
        else:
            await bot.send_message(channel_id, f"Пропущено: **{skipped}**. Очередь пуста.")

    # ── !stop ──────────────────────────────────────────────────────────────
    elif cmd == "!stop":
        async with _guild_lock(guild_id):
            player = _players.get(guild_id)
            if not player:
                await bot.send_message(channel_id, "Бот не в голосовом канале.")
                return
            await player.stop()
            await disconnect_player(guild_id)
        await bot.send_message(channel_id, "Остановлено. Вышел из голосового канала.")

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

        queued = player.queue
        for i, t in enumerate(queued[:20], 1):
            d = _fmt_duration(t.duration) if t.duration else "?"
            lines.append(f"{i}. {t.title} [{d}] — {t.requested_by}")
        if len(queued) > 20:
            lines.append(f"...и ещё {len(queued) - 20}")

        await bot.send_message(channel_id, "\n".join(lines))


if __name__ == "__main__":
    bot.start()
