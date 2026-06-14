import asyncio
import json
import logging
import os
import ssl
from typing import Callable, Awaitable, Any

import aiohttp
import certifi
import websockets
from dotenv import load_dotenv

_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"))

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("FLOODILKA_API", "https://floodilka.com/api/v1")
GATEWAY_URL = os.environ.get("FLOODILKA_GATEWAY", "wss://gateway.floodilka.com/?v=1&encoding=json")

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
        self._ws: Any = None
        self._heartbeat_interval: float = 41250
        self._sequence: int | None = None
        self._session_id: str | None = None
        self._resume_gateway_url: str | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._handlers: dict[str, list[Handler]] = {}

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

    # ── HTTP ────────────────────────────────────────────────────────────────

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
            self._http = aiohttp.ClientSession(
                headers={"Authorization": f"Bot {self.token}"},
                connector=connector,
            )
        return self._http

    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        session = await self._session()
        url = f"{BASE_URL}{endpoint}"
        async with session.request(method, url, **kwargs) as resp:
            resp.raise_for_status()
            if resp.content_type == "application/json":
                return await resp.json()
            return await resp.text()

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

    async def join_voice(self, guild_id: str, channel_id: str) -> dict:
        """Send op=4 to join a voice channel and wait for VOICE_SERVER_UPDATE.

        Returns the VOICE_SERVER_UPDATE payload (contains LiveKit URL + token).
        """
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._voice_server_futures[guild_id] = future

        await self._send(OP_VOICE_STATE_UPDATE, {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "self_mute": False,
            "self_deaf": False,
        })

        return await asyncio.wait_for(future, timeout=10.0)

    async def leave_voice(self, guild_id: str) -> None:
        await self._send(OP_VOICE_STATE_UPDATE, {
            "guild_id": guild_id,
            "channel_id": None,
            "self_mute": False,
            "self_deaf": False,
        })

    # ── Gateway send ────────────────────────────────────────────────────────

    async def _send(self, op: int, data: Any) -> None:
        await self._ws.send(json.dumps({"op": op, "d": data}))

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
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval / 1000)
                await self._send(OP_HEARTBEAT, self._sequence)
                logger.debug("Heartbeat sent (seq=%s)", self._sequence)
        except asyncio.CancelledError:
            pass

    # ── Event dispatch ──────────────────────────────────────────────────────

    async def _dispatch(self, event: str, data: dict) -> None:
        for handler in self._handlers.get(event, []):
            async def _run(h=handler, d=data, e=event):
                try:
                    await h(d)
                except Exception:
                    logger.exception("Error in handler for %s", e)
            asyncio.create_task(_run())

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
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            if self._session_id and self._resume_gateway_url:
                await self._resume()
            else:
                await self._identify()

        elif op == OP_HEARTBEAT_ACK:
            logger.debug("Heartbeat ACK")

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
            await asyncio.sleep(2)
            await self._identify()

        elif op == OP_DISPATCH and event:
            if event == "READY":
                self._session_id = data.get("session_id")
                self._resume_gateway_url = data.get("resume_gateway_url")
                user = data.get("user", {})
                self._bot_id = user.get("id")
                logger.info("Bot ready: %s (id=%s)", user.get("username"), self._bot_id)

            elif event == "GUILD_CREATE":
                guild_id = data.get("id")
                if guild_id:
                    guild_states = self._voice_states.setdefault(guild_id, {})
                    for vs in data.get("voice_states", []):
                        uid = vs.get("user_id")
                        cid = vs.get("channel_id")
                        if uid and cid:
                            guild_states[uid] = cid

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

            elif event == "VOICE_SERVER_UPDATE":
                guild_id = data.get("guild_id")
                logger.info("VOICE_SERVER_UPDATE: %s", data)
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
        async with websockets.connect(url, ssl=_ssl_ctx) as ws:
            self._ws = ws
            async for message in ws:
                await self._handle(message)

    async def run(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        while True:
            try:
                await self._connect()
                logger.info("Connection closed, reconnecting in 5s...")
            except Exception:
                logger.exception("Connection error, reconnecting in 5s...")
            finally:
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
            await asyncio.sleep(5)

    def start(self) -> None:
        asyncio.run(self.run())
