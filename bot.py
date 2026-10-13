import asyncio
import json
import logging
import os
import random
import ssl
from typing import Callable, Awaitable, Any

import aiohttp
import certifi
import websockets
from dotenv import load_dotenv

_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"))

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("FLOODILKA_API", "https://floodilka.com/api/v1")
GATEWAY_URL = os.environ.get("FLOODILKA_GATEWAY", "wss://gateway.floodilka.com/?v=1&encoding=json")

HTTP_CONCURRENCY = int(os.environ.get("HTTP_CONCURRENCY", "50"))
HTTP_MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "3"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "20"))
MAX_RETRY_AFTER = 60.0

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())
if os.environ.get("FLOODILKA_SSL_VERIFY", "1") == "0":
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE
    logger.warning("TLS verification disabled (FLOODILKA_SSL_VERIFY=0)")

OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_PRESENCE_UPDATE = 3
OP_VOICE_STATE_UPDATE = 4
OP_RESUME = 6
OP_RECONNECT = 7
OP_REQUEST_GUILD_MEMBERS = 8
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11
OP_GATEWAY_ERROR = 12

Handler = Callable[[dict], Awaitable[None]]


class FloodilkaBot:
    def __init__(self, token: str, intents: int = 32767):
        self.token = token
        self.intents = intents
        self._http: aiohttp.ClientSession | None = None
        self._http_sem = asyncio.Semaphore(HTTP_CONCURRENCY)
        self._ws: Any = None
        self._heartbeat_interval: float = 41250
        self._sequence: int | None = None
        self._session_id: str | None = None
        self._resume_gateway_url: str | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._ack_pending = False
        self._ready_once = False
        self._handlers: dict[str, list[Handler]] = {}
        # create_task returns a weak reference; without this the GC can kill
        # a handler mid-flight under load.
        self._tasks: set[asyncio.Task] = set()

        self._bot_id: str | None = None
        # {guild_id: {user_id: channel_id}}
        self._voice_states: dict[str, dict[str, str | None]] = {}
        # pending futures for VOICE_SERVER_UPDATE per guild
        self._voice_server_futures: dict[str, asyncio.Future] = {}

    # ── decorator ──────────────────────────────────────────────────────────

    def on(self, event: str):
        def decorator(func: Handler) -> Handler:
            self._handlers.setdefault(event.upper(), []).append(func)
            return func
        return decorator

    def spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # ── HTTP ────────────────────────────────────────────────────────────────

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            connector = aiohttp.TCPConnector(
                ssl=_ssl_ctx,
                limit=HTTP_CONCURRENCY * 4,
                limit_per_host=HTTP_CONCURRENCY * 2,
                ttl_dns_cache=300,
                keepalive_timeout=60,
            )
            self._http = aiohttp.ClientSession(
                headers={"Authorization": f"Bot {self.token}"},
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            )
        return self._http

    async def _retry_after(self, resp: aiohttp.ClientResponse) -> float:
        header = resp.headers.get("Retry-After")
        if header:
            try:
                return min(float(header), MAX_RETRY_AFTER)
            except ValueError:
                pass
        try:
            body = await resp.json()
            if isinstance(body, dict) and "retry_after" in body:
                return min(float(body["retry_after"]), MAX_RETRY_AFTER)
        except Exception:
            pass
        return 1.0

    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        session = await self._session()
        url = f"{BASE_URL}{endpoint}"

        for attempt in range(HTTP_MAX_RETRIES + 1):
            backoff = min(2 ** attempt, 10) + random.uniform(0, 0.5)
            try:
                async with self._http_sem:
                    async with session.request(method, url, **kwargs) as resp:
                        if resp.status == 429 and attempt < HTTP_MAX_RETRIES:
                            delay = await self._retry_after(resp)
                            logger.warning("Rate limited on %s %s, retry in %.2fs",
                                           method, endpoint, delay)
                            await asyncio.sleep(delay)
                            continue
                        if resp.status >= 500 and attempt < HTTP_MAX_RETRIES:
                            logger.warning("HTTP %s on %s %s, retry in %.2fs",
                                           resp.status, method, endpoint, backoff)
                            await asyncio.sleep(backoff)
                            continue
                        resp.raise_for_status()
                        if resp.content_type == "application/json":
                            return await resp.json()
                        return await resp.text()
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                if attempt >= HTTP_MAX_RETRIES:
                    raise
                logger.warning("Network error on %s %s, retry in %.2fs",
                               method, endpoint, backoff)
                await asyncio.sleep(backoff)

        raise RuntimeError(f"Request failed after retries: {method} {endpoint}")

    # ── REST ────────────────────────────────────────────────────────────────

    async def get_me(self) -> dict:
        return await self._request("GET", "/users/@me")

    async def send_message(self, channel_id: str, content: str) -> dict:
        return await self._request(
            "POST", f"/channels/{channel_id}/messages",
            json={"content": content},
        )

    async def edit_message(self, channel_id: str, message_id: str, content: str) -> dict:
        return await self._request(
            "PATCH", f"/channels/{channel_id}/messages/{message_id}",
            json={"content": content},
        )

    async def delete_message(self, channel_id: str, message_id: str) -> None:
        await self._request("DELETE", f"/channels/{channel_id}/messages/{message_id}")

    async def get_channel(self, channel_id: str) -> dict:
        return await self._request("GET", f"/channels/{channel_id}")

    async def get_guild(self, guild_id: str) -> dict:
        return await self._request("GET", f"/guilds/{guild_id}")

    async def get_guild_channels(self, guild_id: str) -> list:
        return await self._request("GET", f"/guilds/{guild_id}/channels")

    # ── Voice helpers ───────────────────────────────────────────────────────

    def get_user_voice_channel(self, guild_id: str, user_id: str) -> str | None:
        return self._voice_states.get(guild_id, {}).get(user_id)

    async def join_voice(self, guild_id: str, channel_id: str, timeout: float = 15.0) -> dict:
        """Send op=4 to join a voice channel and wait for VOICE_SERVER_UPDATE.

        Returns the VOICE_SERVER_UPDATE payload (contains LiveKit URL + token).
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._voice_server_futures[guild_id] = future

        try:
            await self._send(OP_VOICE_STATE_UPDATE, {
                "guild_id": guild_id,
                "channel_id": channel_id,
                "self_mute": False,
                "self_deaf": False,
            })
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._voice_server_futures.pop(guild_id, None)

    async def leave_voice(self, guild_id: str) -> None:
        await self._send(OP_VOICE_STATE_UPDATE, {
            "guild_id": guild_id,
            "channel_id": None,
            "self_mute": False,
            "self_deaf": False,
        })

    # ── Gateway send ────────────────────────────────────────────────────────

    async def _send(self, op: int, data: Any) -> None:
        ws = self._ws
        if ws is None:
            raise ConnectionError("Gateway is not connected")
        await ws.send(json.dumps({"op": op, "d": data}))

    async def _identify(self) -> None:
        await self._send(OP_IDENTIFY, {
            "token": self.token,
            "properties": {"os": "linux", "browser": "floodilka-py", "device": "floodilka-py"},
        })

    async def _resume(self) -> None:
        await self._send(OP_RESUME, {
            "token": self.token,
            "session_id": self._session_id,
            "seq": self._sequence,
        })

    async def _heartbeat_loop(self) -> None:
        interval = self._heartbeat_interval / 1000
        try:
            await asyncio.sleep(interval * random.random())
            while True:
                self._ack_pending = True
                await self._send(OP_HEARTBEAT, self._sequence)
                await asyncio.sleep(interval)
                if self._ack_pending:
                    logger.warning("No heartbeat ACK — connection is a zombie, reconnecting")
                    if self._ws is not None:
                        await self._ws.close(4000)
                    return
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Heartbeat loop died")

    # ── Event dispatch ──────────────────────────────────────────────────────

    async def _dispatch(self, event: str, data: dict) -> None:
        for handler in self._handlers.get(event, []):
            async def _run(h=handler, d=data, e=event):
                try:
                    await h(d)
                except Exception:
                    logger.exception("Error in handler for %s", e)
            self.spawn(_run())

    async def _handle(self, raw: str) -> None:
        msg = json.loads(raw)
        op: int = msg["op"]
        data = msg.get("d")
        seq = msg.get("s")
        event: str | None = msg.get("t")

        if seq is not None:
            self._sequence = seq

        if op == OP_HELLO:
            self._heartbeat_interval = data["heartbeat_interval"]
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            self._ack_pending = False
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            if self._session_id and self._resume_gateway_url:
                await self._resume()
            else:
                await self._identify()

        elif op == OP_HEARTBEAT_ACK:
            self._ack_pending = False

        elif op == OP_HEARTBEAT:
            await self._send(OP_HEARTBEAT, self._sequence)

        elif op == OP_RECONNECT:
            logger.info("Server requested reconnect")
            await self._ws.close(1001)

        elif op == OP_INVALID_SESSION:
            resumable: bool = bool(data)
            logger.warning("Invalid session (resumable=%s)", resumable)
            if not resumable:
                self._session_id = None
                self._resume_gateway_url = None
                self._sequence = None
            await asyncio.sleep(1 + random.random() * 4)
            await self._identify()

        elif op == OP_DISPATCH and event:
            if event == "READY":
                self._session_id = data.get("session_id")
                self._resume_gateway_url = data.get("resume_gateway_url")
                user = data.get("user", {})
                self._bot_id = user.get("id")
                self._ready_once = True
                logger.info("Bot ready: %s (id=%s)", user.get("username"), self._bot_id)

            elif event == "RESUMED":
                self._ready_once = True
                logger.info("Session resumed")

            elif event == "GUILD_CREATE":
                guild_id = data.get("id")
                if guild_id:
                    guild_states = self._voice_states.setdefault(guild_id, {})
                    for vs in data.get("voice_states", []):
                        uid = vs.get("user_id")
                        cid = vs.get("channel_id")
                        if uid and cid:
                            guild_states[uid] = cid

            elif event == "GUILD_DELETE":
                self._voice_states.pop(data.get("id"), None)

            elif event == "VOICE_STATE_UPDATE":
                guild_id = data.get("guild_id")
                user_id = data.get("user_id")
                channel_id = data.get("channel_id")
                if guild_id and user_id:
                    guild_states = self._voice_states.setdefault(guild_id, {})
                    if channel_id:
                        guild_states[user_id] = channel_id
                    else:
                        guild_states.pop(user_id, None)
                        if not guild_states:
                            self._voice_states.pop(guild_id, None)

            elif event == "VOICE_SERVER_UPDATE":
                guild_id = data.get("guild_id")
                logger.info("VOICE_SERVER_UPDATE guild=%s endpoint=%s",
                            guild_id, data.get("endpoint") or data.get("url"))
                future = self._voice_server_futures.pop(guild_id, None)
                if future and not future.done():
                    future.set_result(data)

        elif op == OP_GATEWAY_ERROR:
            logger.error("Gateway error: %s", data)

        if op == OP_DISPATCH and event:
            await self._dispatch(event, data)

    # ── Connection lifecycle ────────────────────────────────────────────────

    async def _connect(self) -> None:
        url = self._resume_gateway_url or GATEWAY_URL
        # App-level heartbeats already detect dead links; WS pings would only
        # add false positives if the gateway does not answer them.
        async with websockets.connect(
            url,
            ssl=_ssl_ctx,
            ping_interval=None,
            open_timeout=30,
            close_timeout=10,
            max_size=16 * 1024 * 1024,
            max_queue=1024,
        ) as ws:
            self._ws = ws
            try:
                async for message in ws:
                    await self._handle(message)
            finally:
                self._ws = None

    async def run(self) -> None:
        backoff = 1.0
        while True:
            self._ready_once = False
            try:
                await self._connect()
                logger.info("Connection closed, reconnecting...")
            except Exception:
                logger.exception("Connection error, reconnecting...")
            finally:
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None
                for fut in self._voice_server_futures.values():
                    if not fut.done():
                        fut.cancel()
                self._voice_server_futures.clear()
            # Only a session that actually reached READY counts as healthy —
            # otherwise a rejected token would reconnect in a tight loop.
            backoff = 1.0 if self._ready_once else min(backoff * 2, 60.0)
            await asyncio.sleep(backoff + random.uniform(0, 1))

    def start(self) -> None:
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        asyncio.run(self.run())
