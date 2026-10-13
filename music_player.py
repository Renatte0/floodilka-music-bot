import asyncio
import logging
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import yt_dlp
from livekit import rtc

logger = logging.getLogger(__name__)

SAMPLE_RATE = 48000
NUM_CHANNELS = 2
SAMPLES_PER_CHANNEL = 960          # 20 ms per frame at 48 kHz
FRAME_BYTES = SAMPLES_PER_CHANNEL * NUM_CHANNELS * 2   # 16-bit PCM

# Buffer LiveKit holds for us. capture_frame() blocks once it is full, which is
# what paces playback — no per-frame timers, so hundreds of streams stay cheap.
AUDIO_BUFFER_MS = int(os.environ.get("AUDIO_BUFFER_MS", "500"))
MAX_QUEUE_LEN = int(os.environ.get("MAX_QUEUE_LEN", "100"))
SEARCH_CONCURRENCY = int(os.environ.get("SEARCH_CONCURRENCY", "8"))
BLOCKING_WORKERS = int(os.environ.get("BLOCKING_WORKERS", "32"))
START_CONCURRENCY = int(os.environ.get("START_CONCURRENCY", "8"))

_PIPE_LIMIT = 1 << 20

_executor = ThreadPoolExecutor(max_workers=BLOCKING_WORKERS, thread_name_prefix="blocking")
_search_sem = asyncio.Semaphore(SEARCH_CONCURRENCY)
# Spreads the cost of spawning FFmpeg and priming buffers so a mass reconnect
# cannot stall the loop for everyone already playing.
_start_sem = asyncio.Semaphore(START_CONCURRENCY)

_ym_client = None
_ym_lock = threading.Lock()


def _get_ym_client():
    global _ym_client
    if _ym_client is not None:
        return _ym_client
    token = os.environ.get("YANDEX_MUSIC_TOKEN", "")
    if not token:
        return None
    with _ym_lock:
        if _ym_client is not None:
            return _ym_client
        try:
            import yandex_music
            _ym_client = yandex_music.Client(token).init()
            logger.info("Yandex Music client ready")
        except Exception as e:
            logger.error("Yandex Music init failed: %s", e)
    return _ym_client


class QueueFull(RuntimeError):
    pass


@dataclass
class Track:
    title: str
    stream_url: str
    webpage_url: str
    duration: int          # seconds
    requested_by: str
    http_headers: dict = field(default_factory=dict)


async def resolve_track(query: str, requested_by: str) -> Track:
    """Search Yandex Music (or resolve a direct URL) and return a Track."""
    loop = asyncio.get_running_loop()
    is_url = query.startswith("http")

    if not is_url:
        def _ym_search():
            client = _get_ym_client()
            if not client:
                return None
            result = client.search(query, type_="track")
            if not result or not result.tracks or not result.tracks.results:
                return None
            t = result.tracks.results[0]
            infos = t.get_download_info(get_direct_links=True)
            if not infos:
                return None
            infos.sort(key=lambda x: x.bitrate_in_kbps or 0, reverse=True)
            url = infos[0].direct_link
            if not url:
                return None
            artists = ", ".join(a.name for a in (t.artists or []))
            title = f"{artists} - {t.title}" if artists else (t.title or "Unknown")
            return Track(
                title=title,
                stream_url=url,
                webpage_url=f"https://music.yandex.ru/track/{t.id}",
                duration=(t.duration_ms or 0) // 1000,
                requested_by=requested_by,
            )

        try:
            async with _search_sem:
                track = await loop.run_in_executor(_executor, _ym_search)
            if track:
                return track
        except Exception as e:
            logger.error("Yandex Music search error: %s", e)

        raise RuntimeError(f"Не удалось найти трек: {query}")

    ydl_opts = {
        "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "no_warnings": True,
        "socket_timeout": 15,
    }

    def _extract() -> dict:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(query, download=False)
            if "entries" in info:
                info = info["entries"][0]
            return info

    async with _search_sem:
        info = await loop.run_in_executor(_executor, _extract)
    return Track(
        title=info.get("title", "Unknown"),
        stream_url=info["url"],
        webpage_url=info.get("webpage_url", query),
        duration=info.get("duration", 0),
        requested_by=requested_by,
        http_headers=info.get("http_headers", {}),
    )


def _fmt_duration(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _build_ffmpeg_args(track: Track) -> list[str]:
    headers_str = ""
    for k, v in track.http_headers.items():
        headers_str += f"{k}: {v}\r\n"

    args = ["ffmpeg", "-nostdin", "-threads", "1",
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5"]
    if headers_str:
        args += ["-headers", headers_str]
    args += [
        "-i", track.stream_url,
        "-vn",
        "-f", "s16le",
        "-ar", str(SAMPLE_RATE),
        "-ac", str(NUM_CHANNELS),
        "-loglevel", "quiet",
        "-",
    ]
    return args


def _spawn_ffmpeg(ffmpeg_args: list[str]) -> subprocess.Popen:
    """Start FFmpeg synchronously — called from a thread via run_in_executor
    to avoid fork-after-thread deadlock on macOS with LiveKit Rust threads."""
    return subprocess.Popen(
        ffmpeg_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,
    )


def _make_frame(data: bytes) -> rtc.AudioFrame:
    return rtc.AudioFrame(
        data=data,
        sample_rate=SAMPLE_RATE,
        num_channels=NUM_CHANNELS,
        samples_per_channel=SAMPLES_PER_CHANNEL,
    )


class MusicPlayer:
    def __init__(self):
        self._room: rtc.Room | None = None
        self._source: rtc.AudioSource | None = None
        self._publication: rtc.LocalTrackPublication | None = None
        self._queue: list[Track] = []
        self._current: Track | None = None
        self._play_task: asyncio.Task | None = None
        self._proc: subprocess.Popen | None = None
        self._transport: asyncio.ReadTransport | None = None
        self._paused = False
        self._stopping = False
        self._dead = False

    # ── Properties ─────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        if self._room is None or self._dead:
            return False
        try:
            return self._room.isconnected()
        except Exception:
            return False

    @property
    def current(self) -> Track | None:
        return self._current

    @property
    def queue(self) -> list[Track]:
        return list(self._queue)

    @property
    def is_playing(self) -> bool:
        return self._current is not None and not self._paused

    @property
    def is_paused(self) -> bool:
        return self._paused

    # ── Lifecycle ───────────────────────────────────────────────────────────

    async def connect(self, url: str, token: str) -> None:
        """Connect and publish one reusable audio track for the whole session."""
        room = rtc.Room()

        def _on_disconnected(*_args):
            logger.warning("LiveKit room dropped")
            self._dead = True

        room.on("disconnected", _on_disconnected)
        await asyncio.wait_for(room.connect(url, token), timeout=20.0)
        self._room = room

        self._source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS,
                                       queue_size_ms=AUDIO_BUFFER_MS)
        lk_track = rtc.LocalAudioTrack.create_audio_track("music", self._source)
        opts = rtc.TrackPublishOptions()
        opts.source = rtc.TrackSource.SOURCE_MICROPHONE
        self._publication = await asyncio.wait_for(
            room.local_participant.publish_track(lk_track, opts), timeout=15.0
        )
        logger.info("LiveKit room connected, audio track published")

    async def disconnect(self) -> None:
        self._stopping = True
        try:
            self._queue.clear()
            await self._cancel_playback()
            self._current = None
            if self._room:
                try:
                    await asyncio.wait_for(self._room.disconnect(), timeout=10.0)
                except Exception:
                    logger.warning("LiveKit disconnect failed", exc_info=True)
            self._room = None
            self._source = None
            self._publication = None
        finally:
            self._stopping = False

    # ── Queue control ───────────────────────────────────────────────────────

    def enqueue(self, track: Track) -> None:
        if len(self._queue) >= MAX_QUEUE_LEN:
            raise QueueFull(f"Очередь переполнена (максимум {MAX_QUEUE_LEN} треков).")
        self._queue.append(track)

    async def play(self) -> bool:
        """Start playing if idle. Returns True if playback started."""
        if self._current is not None:
            return False
        return await self._play_next()

    async def stop(self) -> None:
        self._stopping = True
        try:
            self._queue.clear()
            await self._cancel_playback()
            self._current = None
        finally:
            self._stopping = False

    async def skip(self) -> Track | None:
        """Skip current track. Returns the next track or None."""
        await self._cancel_playback()
        self._current = None
        if not self._stopping:
            await self._play_next()
        return self._current

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    # ── Internal ────────────────────────────────────────────────────────────

    def _close_stream(self) -> None:
        """Synchronous so it is safe to run from a finally block during cancellation."""
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None
        proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
            _executor.submit(proc.wait)

    async def _cancel_playback(self) -> None:
        task, self._play_task = self._play_task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Playback task failed during cancel")
        self._close_stream()
        if self._source is not None:
            try:
                self._source.clear_queue()
            except Exception:
                pass

    async def _play_next(self) -> bool:
        if not self._queue or self._room is None or self._dead:
            self._current = None
            return False
        self._current = self._queue.pop(0)
        self._play_task = asyncio.create_task(self._stream_loop())
        return True

    async def _open_stream(self, track: Track):
        loop = asyncio.get_running_loop()
        async with _start_sem:
            proc = await asyncio.wait_for(
                loop.run_in_executor(_executor, _spawn_ffmpeg, _build_ffmpeg_args(track)),
                timeout=20.0,
            )
            reader = asyncio.StreamReader(limit=_PIPE_LIMIT)
            transport, _ = await loop.connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(reader), proc.stdout
            )
        return proc, transport, reader

    async def _stream_loop(self) -> None:
        track = self._current
        logger.info("Playing: %s", track.title)

        try:
            proc, transport, reader = await self._open_stream(track)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("FFmpeg start failed for %s", track.title)
            self._current = None
            if not self._stopping:
                await self._play_next()
            return

        self._proc, self._transport = proc, transport
        cancelled = False
        try:
            while not self._stopping and not self._dead:
                if self._paused:
                    await asyncio.sleep(0.1)
                    continue
                try:
                    data = await reader.readexactly(FRAME_BYTES)
                except asyncio.IncompleteReadError as e:
                    tail = bytes(e.partial)
                    if tail:
                        await self._source.capture_frame(
                            _make_frame(tail + b"\x00" * (FRAME_BYTES - len(tail)))
                        )
                    break
                await self._source.capture_frame(_make_frame(data))
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception:
            logger.exception("Playback error on %s", track.title)
        finally:
            self._close_stream()
            if not cancelled and not self._stopping:
                self._current = None
                await self._play_next()
