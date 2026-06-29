import asyncio
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field

import yt_dlp
from livekit import rtc

logger = logging.getLogger(__name__)

_ym_client = None


def _get_ym_client():
    global _ym_client
    if _ym_client is not None:
        return _ym_client
    token = os.environ.get("YANDEX_MUSIC_TOKEN", "")
    if not token:
        return None
    try:
        import yandex_music
        _ym_client = yandex_music.Client(token).init()
        logger.info("Yandex Music client ready")
    except Exception as e:
        logger.error("Yandex Music init failed: %s", e)
    return _ym_client

SAMPLE_RATE = 48000
NUM_CHANNELS = 2
SAMPLES_PER_CHANNEL = 960          # 20 ms per frame at 48 kHz
FRAME_BYTES = SAMPLES_PER_CHANNEL * NUM_CHANNELS * 2   # 16-bit PCM


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
    is_url = query.startswith("http")

    if not is_url:
        loop = asyncio.get_event_loop()

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
            track = await loop.run_in_executor(None, _ym_search)
            if track:
                return track
        except Exception as e:
            logger.error("Yandex Music search error: %s", e)

        raise RuntimeError(f"Не удалось найти трек: {query}")

    # Direct URL — fall back to yt-dlp
    ydl_opts = {
        "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "no_warnings": True,
    }
    loop = asyncio.get_event_loop()

    def _extract() -> dict:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(query, download=False)
            if "entries" in info:
                info = info["entries"][0]
            return info

    info = await loop.run_in_executor(None, _extract)
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

    args = ["ffmpeg", "-reconnect", "1", "-reconnect_streamed", "1",
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


def _start_ffmpeg_blocking(ffmpeg_args: list[str]) -> subprocess.Popen:
    """Start FFmpeg synchronously — called from a thread via run_in_executor
    to avoid fork-after-thread deadlock on macOS with LiveKit Rust threads."""
    return subprocess.Popen(
        ffmpeg_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,
    )


def _read_blocking(proc: subprocess.Popen, size: int) -> bytes:
    return proc.stdout.read(size)


class MusicPlayer:
    def __init__(self):
        self._room: rtc.Room | None = None
        self._source: rtc.AudioSource | None = None
        self._track: rtc.LocalAudioTrack | None = None
        self._publication: rtc.LocalTrackPublication | None = None
        self._queue: list[Track] = []
        self._current: Track | None = None
        self._play_task: asyncio.Task | None = None
        self._ffmpeg_proc: subprocess.Popen | None = None
        self._paused = False
        self._stopping = False

    # ── Properties ─────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._room is not None

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
        self._room = rtc.Room()
        await self._room.connect(url, token)
        logger.info("Connected to LiveKit room")

    async def disconnect(self) -> None:
        self._stopping = True
        await self._cancel_playback()
        if self._room:
            await self._room.disconnect()
            self._room = None
        self._stopping = False

    # ── Queue control ───────────────────────────────────────────────────────

    def enqueue(self, track: Track) -> None:
        self._queue.append(track)

    async def play(self) -> bool:
        """Start playing if idle. Returns True if playback started."""
        if self._current is not None:
            return False
        return await self._play_next()

    async def stop(self) -> None:
        self._stopping = True
        self._queue.clear()
        await self._cancel_playback()
        self._current = None
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

    async def _cancel_playback(self) -> None:
        if self._play_task and not self._play_task.done():
            self._play_task.cancel()
            try:
                await self._play_task
            except asyncio.CancelledError:
                pass
        proc = self._ffmpeg_proc
        if proc is not None and proc.poll() is None:
            proc.kill()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, proc.wait)
        self._ffmpeg_proc = None

    async def _play_next(self) -> bool:
        if not self._queue:
            self._current = None
            return False
        self._current = self._queue.pop(0)
        self._play_task = asyncio.create_task(self._stream_loop())
        return True

    async def _stream_loop(self) -> None:
        track = self._current
        logger.info("Playing: %s", track.title)
        loop = asyncio.get_running_loop()

        # Publish LiveKit audio track
        try:
            self._source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
            self._track = rtc.LocalAudioTrack.create_audio_track("music", self._source)
            opts = rtc.TrackPublishOptions()
            opts.source = rtc.TrackSource.SOURCE_MICROPHONE
            self._publication = await asyncio.wait_for(
                self._room.local_participant.publish_track(self._track, opts),
                timeout=10.0,
            )
            logger.info("[dbg] Track published OK")
        except Exception:
            logger.exception("[dbg] publish_track failed")
            return

        # Start FFmpeg in a thread to avoid fork-after-thread deadlock on macOS
        ffmpeg_args = _build_ffmpeg_args(track)
        logger.info("[dbg] Starting FFmpeg via executor...")
        try:
            self._ffmpeg_proc = await asyncio.wait_for(
                loop.run_in_executor(None, _start_ffmpeg_blocking, ffmpeg_args),
                timeout=15.0,
            )
            logger.info("[dbg] FFmpeg started PID=%s", self._ffmpeg_proc.pid)
        except asyncio.TimeoutError:
            logger.error("[dbg] FFmpeg start timed out")
            return
        except Exception:
            logger.exception("[dbg] FFmpeg start failed")
            return

        FRAME_DURATION = SAMPLES_PER_CHANNEL / SAMPLE_RATE  # 0.02 s
        frames_sent = 0

        try:
            start_time = time.monotonic()
            while True:
                if self._paused:
                    await asyncio.sleep(0.05)
                    start_time = time.monotonic() - frames_sent * FRAME_DURATION
                    continue

                # Read raw PCM from FFmpeg in a thread (non-blocking for event loop)
                data = await loop.run_in_executor(
                    None, _read_blocking, self._ffmpeg_proc, FRAME_BYTES
                )
                if not data:
                    logger.info("[dbg] FFmpeg stdout EOF, track done")
                    break

                if len(data) < FRAME_BYTES:
                    data = data + b"\x00" * (FRAME_BYTES - len(data))

                frame = rtc.AudioFrame(
                    data=data,
                    sample_rate=SAMPLE_RATE,
                    num_channels=NUM_CHANNELS,
                    samples_per_channel=SAMPLES_PER_CHANNEL,
                )
                await self._source.capture_frame(frame)
                frames_sent += 1

                if frames_sent == 1:
                    logger.info("[dbg] First audio frame sent to LiveKit!")

                # Pace to real-time (20 ms per frame)
                next_frame_at = start_time + frames_sent * FRAME_DURATION
                sleep_for = next_frame_at - time.monotonic()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)

        except asyncio.CancelledError:
            proc = self._ffmpeg_proc
            if proc and proc.poll() is None:
                proc.kill()
                await loop.run_in_executor(None, proc.wait)
        finally:
            self._ffmpeg_proc = None
            if self._publication:
                try:
                    await self._room.local_participant.unpublish_track(self._publication.sid)
                except Exception:
                    pass
            self._source = None
            self._track = None
            self._publication = None

            if not self._stopping:
                self._current = None
                await self._play_next()
