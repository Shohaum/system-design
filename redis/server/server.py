"""
Architecture:

    asyncio event loop
        └── asyncio.start_server()  ← listens on TCP port
                └── _handle_client()  ← one coroutine per connection
                        └── RESPParser  ← per-connection parse buffer
                        └── CommandDispatcher  ← shared, one per server

Why asyncio here instead of threading?
    - Redis itself is single-threaded: one event loop, no GIL fights.
    - asyncio gives us concurrency without OS thread overhead.
    - Our Store already has an RLock from Day 2 — it's compatible with
      single-threaded async since we never await while holding data.
    - Day 5's pub/sub is much cleaner with async: subscribers just
      await a queue, publisher pushes to all subscriber queues.

Connection lifecycle:
    1. Client connects → _handle_client() coroutine is spawned
    2. Loop: read bytes → feed parser → extract commands → dispatch → write reply
    3. Pipelining: multiple commands may arrive in one read() — the parser
       handles this via its internal buffer, we loop until it's empty.
    4. Client disconnects (EOF) → coroutine exits cleanly

Error handling:
    - CommandError / StoreError → send RESP error frame, keep connection alive
    - RESPError (malformed bytes) → send error, close connection
    - Any unexpected exception → log + close connection
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .commands import CommandDispatcher, CommandError
from .store import Store, StoreError
from .resp import RESPParser, RESPError, encode, encode_simple_string, encode_error
from .ttl import ExpirySweeperThread

logger = logging.getLogger("mini-redis")


class RedisServer:
    """
    Usage:
        server = RedisServer(host="127.0.0.1", port=6399)
        asyncio.run(server.start())

    Or for testing:
        async with server.run_context():
            # server is up, do stuff
            pass
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6399,
        max_keys: Optional[int] = None,
        sweep_interval: float = 1.0,
    ) -> None:
        self.host = host
        self.port = port
        self._store      = Store(max_keys=max_keys)
        self._dispatcher = CommandDispatcher(store=self._store)
        self._sweeper    = ExpirySweeperThread(self._store, interval=sweep_interval)
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_count = 0

    # Lifecycle

    async def start(self) -> None:
        """Start the server and run forever (Ctrl-C to stop)."""
        self._sweeper.start()
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
        )
        addr = self._server.sockets[0].getsockname()
        logger.info(f"mini-redis listening on {addr[0]}:{addr[1]}")
        print(f"mini-redis ready on {addr[0]}:{addr[1]}")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._sweeper.stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # Used in tests so we can spin up/down cleanly inside an async context
    class _RunContext:
        def __init__(self, server: "RedisServer") -> None:
            self._server = server

        async def __aenter__(self) -> "RedisServer":
            self._server._sweeper.start()
            self._server._server = await asyncio.start_server(
                self._server._handle_client,
                self._server.host,
                self._server.port,
            )
            return self._server

        async def __aexit__(self, *_) -> None:
            await self._server.stop()

    def run_context(self) -> "_RunContext":
        return self._RunContext(self)

    # Per-connection handler

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Coroutine spawned for each TCP connection.

        Reads raw bytes, feeds them into a per-connection RESPParser,
        dispatches complete commands, and writes RESP-encoded replies.
        """
        peer = writer.get_extra_info("peername", "<unknown>")
        self._client_count += 1
        client_id = self._client_count
        logger.debug(f"[{client_id}] connected from {peer}")

        parser = RESPParser()

        try:
            while True:
                # Read up to 64KB at a time.
                # asyncio will suspend here until bytes arrive or EOF.
                data = await reader.read(65536)
                if not data:
                    # EOF — client disconnected cleanly
                    break

                parser.feed(data)

                # Drain all complete commands from the parse buffer.
                # This handles pipelining: N commands in one TCP segment.
                while True:
                    try:
                        tokens = parser.read_command()
                    except RESPError as e:
                        writer.write(encode_error(f"Protocol error: {e}"))
                        await writer.drain()
                        return   # malformed RESP → close connection

                    if tokens is None:
                        break   # need more data from socket

                    if not tokens:
                        continue

                    reply = self._dispatch(tokens)
                    writer.write(reply)

                # Flush all replies to the socket in one syscall.
                # drain() is the asyncio equivalent of socket.flush().
                await writer.drain()

        except ConnectionResetError:
            pass   # client killed connection mid-stream, that's fine
        except Exception as e:
            logger.exception(f"[{client_id}] unexpected error: {e}")
        finally:
            logger.debug(f"[{client_id}] disconnected")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # Command dispatch → RESP bytes

    def _dispatch(self, tokens: list[str]) -> bytes:
        """
        Execute one parsed command and return the RESP-encoded reply bytes.
        Never raises — all errors are caught and encoded as RESP error frames.
        """
        if not tokens:
            return encode_error("Empty command")

        cmd = tokens[0].upper()

        # Handle a few meta-commands that don't go through CommandDispatcher
        if cmd == "PING":
            payload = tokens[1] if len(tokens) > 1 else None
            return encode_simple_string(payload or "PONG")

        if cmd == "QUIT":
            return encode_simple_string("OK")

        if cmd == "COMMAND":
            # redis-cli sends COMMAND DOCS on startup; just reply empty array
            return b"*0\r\n"

        try:
            # Pass the token list directly — no string re-parsing needed.
            result = self._dispatcher.execute_tokens(tokens)
            return self._encode_result(result)

        except CommandError as e:
            return encode_error(str(e))
        except StoreError as e:
            return f"-WRONGTYPE {e}\r\n".encode()
        except (ValueError, TypeError) as e:
            return encode_error(str(e))
        except Exception as e:
            logger.exception(f"Unhandled error for command {tokens!r}: {e}")
            return encode_error("Internal server error")

    def _encode_result(self, result: object) -> bytes:
        """
        Choose the right RESP encoding for a command result.
        "OK" strings get Simple String encoding (+OK\r\n) to match real Redis.
        Everything else goes through the general encoder.
        """
        if result == "OK":
            return encode_simple_string("OK")
        return encode(result)

# Entrypoint

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    server = RedisServer(host="127.0.0.1", port=6399)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()