"""
These test the wire protocol layer in complete isolation — no Store, no network.
Think of them like testing a JSON library: input bytes → Python objects,
and Python objects → output bytes.
"""

import pytest
from redis.server.resp import RESPParser, RESPError, encode, encode_simple_string, encode_error


# Encoder tests

class TestEncode:
    def test_none_encodes_to_null_bulk(self):
        assert encode(None) == b"$-1\r\n"

    def test_string(self):
        assert encode("hello") == b"$5\r\nhello\r\n"

    def test_empty_string(self):
        assert encode("") == b"$0\r\n\r\n"

    def test_string_with_spaces(self):
        assert encode("hello world") == b"$11\r\nhello world\r\n"

    def test_string_with_crlf(self):
        # Bulk strings are binary-safe — \r\n inside the value is fine
        result = encode("foo\r\nbar")
        assert result == b"$8\r\nfoo\r\nbar\r\n"

    def test_integer(self):
        assert encode(42) == b":42\r\n"

    def test_negative_integer(self):
        assert encode(-7) == b":-7\r\n"

    def test_zero(self):
        assert encode(0) == b":0\r\n"

    def test_bool_true(self):
        # bool is a subclass of int — must be checked before int
        assert encode(True) == b":1\r\n"

    def test_bool_false(self):
        assert encode(False) == b":0\r\n"

    def test_list(self):
        result = encode(["SET", "foo", "bar"])
        assert result == b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

    def test_empty_list(self):
        assert encode([]) == b"*0\r\n"

    def test_list_with_none(self):
        result = encode(["a", None, "b"])
        assert result == b"*3\r\n$1\r\na\r\n$-1\r\n$1\r\nb\r\n"

    def test_set_sorted(self):
        # Sets must encode deterministically
        result = encode({"c", "a", "b"})
        assert result == b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"

    def test_exception(self):
        result = encode(Exception("something went wrong"))
        assert result == b"-ERR something went wrong\r\n"

    def test_simple_string_ok(self):
        assert encode_simple_string("OK") == b"+OK\r\n"

    def test_simple_string_pong(self):
        assert encode_simple_string("PONG") == b"+PONG\r\n"

    def test_encode_error(self):
        assert encode_error("bad command") == b"-ERR bad command\r\n"


# Parser: complete commands

class TestParserComplete:
    def _parse(self, data: bytes) -> list[str]:
        p = RESPParser()
        p.feed(data)
        return p.read_command()

    def test_simple_array_command(self):
        data = b"*1\r\n$4\r\nPING\r\n"
        assert self._parse(data) == ["PING"]

    def test_set_command(self):
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        assert self._parse(data) == ["SET", "foo", "bar"]

    def test_get_command(self):
        data = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
        assert self._parse(data) == ["GET", "foo"]

    def test_lpush_multiple_values(self):
        data = b"*5\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        assert self._parse(data) == ["LPUSH", "q", "a", "b", "c"]

    def test_value_with_spaces(self):
        # Bulk strings can contain spaces — unlike inline commands
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n"
        assert self._parse(data) == ["SET", "key", "hello world"]

    def test_value_with_crlf(self):
        # Binary-safe: value contains \r\n literally
        data = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nfoo\r\nbar"
        # Intentionally incomplete — only 5 bytes of 5-byte bulk + no \r\n after
        # Actually let's make it complete:
        data = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$7\r\nfoo\r\nba\r\n"
        result = self._parse(data)
        assert result == ["SET", "k", "foo\r\nba"]

    def test_empty_array(self):
        data = b"*0\r\n"
        assert self._parse(data) == []

    def test_inline_ping(self):
        data = b"PING\r\n"
        assert self._parse(data) == ["PING"]

    def test_inline_with_args(self):
        data = b"SET foo bar\r\n"
        assert self._parse(data) == ["SET", "foo", "bar"]

    def test_returns_none_when_empty(self):
        p = RESPParser()
        assert p.read_command() is None


# Parser: incremental / fragmented input

class TestParserIncremental:
    def test_command_split_across_two_feeds(self):
        p = RESPParser()
        # Send the array header in one chunk
        p.feed(b"*2\r\n$3\r\nGET\r\n")
        assert p.read_command() is None   # incomplete
        # Send the rest
        p.feed(b"$3\r\nfoo\r\n")
        assert p.read_command() == ["GET", "foo"]

    def test_byte_by_byte(self):
        p = RESPParser()
        data = b"*1\r\n$4\r\nPING\r\n"
        for byte in data[:-1]:
            p.feed(bytes([byte]))
            assert p.read_command() is None
        p.feed(data[-1:])
        assert p.read_command() == ["PING"]

    def test_two_commands_in_one_feed(self):
        # Pipelining: two commands arrive in a single read()
        p = RESPParser()
        p.feed(
            b"*1\r\n$4\r\nPING\r\n"
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
        )
        assert p.read_command() == ["PING"]
        assert p.read_command() == ["GET", "foo"]
        assert p.read_command() is None

    def test_three_pipelined_commands(self):
        p = RESPParser()
        p.feed(
            b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
            b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
            b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
        )
        assert p.read_command() == ["SET", "a", "1"]
        assert p.read_command() == ["SET", "b", "2"]
        assert p.read_command() == ["GET", "a"]
        assert p.read_command() is None

    def test_partial_then_complete(self):
        p = RESPParser()
        p.feed(b"*3\r\n$3\r\nSET\r\n")
        assert p.read_command() is None
        p.feed(b"$3\r\nkey\r\n$5\r\nvalue\r\n")
        assert p.read_command() == ["SET", "key", "value"]

    def test_buffer_state_preserved_on_partial(self):
        # After a failed partial parse, the buffer should be intact
        p = RESPParser()
        p.feed(b"*2\r\n$3\r\nGET\r\n")
        p.read_command()   # returns None, buffer preserved
        p.feed(b"$3\r\nfoo\r\n")
        assert p.read_command() == ["GET", "foo"]


# Parser: error cases

class TestParserErrors:
    def test_bad_array_count(self):
        p = RESPParser()
        p.feed(b"*abc\r\n")
        with pytest.raises(RESPError):
            p.read_command()

    def test_bad_bulk_length(self):
        p = RESPParser()
        p.feed(b"*1\r\n$abc\r\nPING\r\n")
        with pytest.raises(RESPError):
            p.read_command()


# Round-trip

class TestRoundTrip:
    """Encode a command as RESP, then parse it back."""

    def _roundtrip(self, tokens: list[str]) -> list[str]:
        # Encode as a RESP array of bulk strings
        parts = [f"*{len(tokens)}\r\n".encode()]
        for t in tokens:
            enc = t.encode()
            parts.append(f"${len(enc)}\r\n".encode() + enc + b"\r\n")
        wire = b"".join(parts)

        p = RESPParser()
        p.feed(wire)
        return p.read_command()

    def test_set_roundtrip(self):
        assert self._roundtrip(["SET", "key", "value"]) == ["SET", "key", "value"]

    def test_lpush_roundtrip(self):
        assert self._roundtrip(["LPUSH", "q", "a", "b", "c"]) == ["LPUSH", "q", "a", "b", "c"]

    def test_value_with_special_chars(self):
        tokens = ["SET", "k", "hello world\nfoo"]
        assert self._roundtrip(tokens) == tokens