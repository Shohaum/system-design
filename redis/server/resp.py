"""
RESP is the wire protocol Redis uses over TCP. It's dead simple and worth
understanding fully — it's a great example of a binary-safe text protocol.

RESP2 data types:
    +OK\r\n                         → Simple String  "OK"
    -ERR something\r\n             → Error          raises on client side
    :42\r\n                        → Integer        42
    $5\r\nHello\r\n                → Bulk String     "Hello"  (length-prefixed)
    $-1\r\n                        → Null Bulk      None
    *3\r\n$3\r\nSET\r\n...         → Array          ["SET", "key", "val"]

Why length-prefixed bulk strings?
    Because values can contain \r\n literally (e.g. a stored JSON blob).
    Simple strings can't — they terminate at the first \r\n.
    Bulk strings say "the next N bytes are the value" so binary data is safe.

How a redis-cli command looks on the wire:
    SET foo bar  →  *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    GET foo      →  *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n

Parser design — incremental / streaming:
    RESPParser wraps a bytearray buffer. You feed() chunks of bytes into it
    as they arrive from the socket. read_command() tries to parse one complete
    command from the buffer; if there isn't enough data yet it returns None.
    This handles TCP fragmentation and pipelining naturally.

    Pipelining: a client can send N commands without waiting for replies.
    Our buffer may contain multiple complete commands — the server calls
    read_command() in a loop until it returns None.
"""

from __future__ import annotations

class RESPError(Exception):
    """Raised when the incoming byte stream violates the RESP protocol."""
    pass


def encode(value: object) -> bytes:
    """
    Serialize a Python value into RESP bytes to send to the client.

    Type mapping:
        str           → Bulk String  (safe for any content)
        int / float   → Integer      (float rounded to int, matching Redis)
        list / tuple  → Array        (each element encoded recursively)
        set           → Array        (sorted for deterministic output)
        bool          → Integer 1/0  (must come before int — bool is int subclass)
        None          → Null Bulk String
        Exception     → Error
    """
    if isinstance(value, Exception):
        msg = str(value).replace("\r", "").replace("\n", " ")
        return f"-ERR {msg}\r\n".encode()

    if isinstance(value, bool):
        return f":{1 if value else 0}\r\n".encode()

    if isinstance(value, int):
        return f":{value}\r\n".encode()

    if isinstance(value, float):
        return f":{int(value)}\r\n".encode()

    if value is None:
        return b"$-1\r\n"

    if isinstance(value, str):
        encoded = value.encode()
        return f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"

    if isinstance(value, bytes):
        return f"${len(value)}\r\n".encode() + value + b"\r\n"

    if isinstance(value, (list, tuple)):
        parts = [f"*{len(value)}\r\n".encode()]
        for item in value:
            parts.append(encode(item))
        return b"".join(parts)

    if isinstance(value, set):
        # Sort for deterministic wire output (sets are unordered)
        items = sorted(value)
        parts = [f"*{len(items)}\r\n".encode()]
        for item in items:
            parts.append(encode(item))
        return b"".join(parts)

    # Fallback: coerce to string
    return encode(str(value))


def encode_simple_string(s: str) -> bytes:
    """
    Encode as a Simple String (+OK\r\n style).
    Only safe for values that never contain \r or \n.
    Used for status replies like OK, PONG.
    """
    return f"+{s}\r\n".encode()


def encode_error(msg: str) -> bytes:
    """Encode a RESP error frame."""
    msg = msg.replace("\r", "").replace("\n", " ")
    return f"-ERR {msg}\r\n".encode()


#   Parser

class RESPParser:
    """
    Incremental RESP parser.

    Feed incoming bytes with feed(). Call read_command() to extract
    one complete command (as a list of str tokens) from the buffer.
    Returns None if there isn't a complete command yet.

    Usage:
        parser = RESPParser()
        parser.feed(data)               # data from socket read
        while (cmd := parser.read_command()) is not None:
            handle(cmd)                 # cmd is e.g. ["SET", "foo", "bar"]
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        """Append raw bytes from the socket into the internal buffer."""
        self._buf.extend(data)

    def read_command(self) -> list[str] | None:
        """
        Try to parse one complete RESP command from the buffer.
        Returns the command as a list of strings, or None if incomplete.
        Advances the buffer past consumed bytes on success.
        Raises RESPError on malformed input.
        """
        if not self._buf:
            return None

        # Redis clients send commands as RESP Arrays of Bulk Strings.
        # Inline commands (plain text like "PING\r\n") are also supported
        # for telnet / simple testing.
        if self._buf[0:1] == b"*":
            return self._parse_array()
        else:
            return self._parse_inline()

    # Internal parsing helpers

    def _find_crlf(self, start: int = 0) -> int:
        """
        Return the index of the next \r\n in the buffer, or -1 if not found.
        This is O(n) but buffers are small (one command at a time).
        """
        idx = self._buf.find(b"\r\n", start)
        return idx

    def _read_line(self) -> str | None:
        """
        Read one \r\n-terminated line from the buffer.
        Returns the line content (without \r\n), or None if incomplete.
        Advances the buffer on success.
        """
        idx = self._find_crlf()
        if idx == -1:
            return None
        line = self._buf[:idx].decode(errors="replace")
        del self._buf[:idx + 2]   # consume line + \r\n
        return line

    def _read_bulk_string(self, length: int) -> str | None:
        """
        Read exactly `length` bytes + trailing \r\n from the buffer.
        Returns the decoded string, or None if not enough data yet.
        """
        needed = length + 2   # +2 for \r\n
        if len(self._buf) < needed:
            return None
        data = bytes(self._buf[:length])
        if self._buf[length:length + 2] != b"\r\n":
            raise RESPError(f"Expected \\r\\n after bulk string, got {self._buf[length:length+2]!r}")
        del self._buf[:needed]
        return data.decode(errors="replace")

    def _parse_array(self) -> list[str] | None:
        """
        Parse a RESP Array: *<count>\r\n followed by <count> bulk strings.
        Saves and restores the buffer if we don't have a complete command yet
        (so a partial parse doesn't corrupt state).
        """
        # Snapshot buffer in case we need to roll back
        snapshot = bytes(self._buf)

        line = self._read_line()
        if line is None:
            self._buf = bytearray(snapshot)
            return None

        if not line.startswith("*"):
            raise RESPError(f"Expected array prefix '*', got {line!r}")

        try:
            count = int(line[1:])
        except ValueError:
            raise RESPError(f"Invalid array count: {line[1:]!r}")

        if count == -1:
            return None   # null array

        if count == 0:
            return []

        tokens: list[str] = []
        for _ in range(count):
            # Each element must be a bulk string
            header = self._read_line()
            if header is None:
                self._buf = bytearray(snapshot)
                return None

            if not header.startswith("$"):
                raise RESPError(f"Expected bulk string '$', got {header!r}")

            try:
                length = int(header[1:])
            except ValueError:
                raise RESPError(f"Invalid bulk string length: {header[1:]!r}")

            if length == -1:
                tokens.append(None)   # null bulk string within array
                continue

            value = self._read_bulk_string(length)
            if value is None:
                self._buf = bytearray(snapshot)
                return None

            tokens.append(value)

        return tokens

    def _parse_inline(self) -> list[str] | None:
        """
        Parse an inline command: a plain space-separated line ending in \r\n.
        Example: "PING\r\n"  →  ["PING"]
                 "SET k v\r\n" → ["SET", "k", "v"]

        Useful for telnet debugging — real clients always send RESP arrays.
        """
        snapshot = bytes(self._buf)
        line = self._read_line()
        if line is None:
            self._buf = bytearray(snapshot)
            return None
        tokens = line.strip().split()
        return tokens if tokens else None