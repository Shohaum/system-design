import asyncio
import sys
sys.path.insert(0, ".")

from redis.server.server import RedisServer
from redis.server.resp import encode_simple_string


class RESPClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

    async def send(self, *args):
        frame = f"*{len(args)}\r\n".encode()
        for arg in args:
            enc = arg.encode()
            frame += f"${len(enc)}\r\n".encode() + enc + b"\r\n"
        self._writer.write(frame)
        await self._writer.drain()
        return await self._read_response()

    async def _read_response(self):
        line = await self._reader.readline()
        line = line.rstrip(b"\r\n")
        if line.startswith(b"+"):
            return line[1:].decode()
        if line.startswith(b"-"):
            return Exception(line[1:].decode())
        if line.startswith(b":"):
            return int(line[1:])
        if line.startswith(b"$"):
            length = int(line[1:])
            if length == -1:
                return None
            data = await self._reader.readexactly(length + 2)
            return data[:-2].decode()
        if line.startswith(b"*"):
            count = int(line[1:])
            if count == -1:
                return None
            return [await self._read_response() for _ in range(count)]
        raise ValueError(f"Unknown RESP prefix: {line!r}")


async def run_tests():
    passed = 0
    failed = 0

    def check(name, got, expected):
        nonlocal passed, failed
        if got == expected:
            print(f"  ✓ {name}")
            passed += 1
        else:
            print(f"  ✗ {name}: got {got!r} expected {expected!r}")
            failed += 1

    def check_range(name, val, lo, hi):
        nonlocal passed, failed
        if lo <= val <= hi:
            print(f"  ✓ {name} ({val} in [{lo},{hi}])")
            passed += 1
        else:
            print(f"  ✗ {name}: {val!r} not in [{lo},{hi}]")
            failed += 1

    server = RedisServer(host="127.0.0.1", port=0)

    async with server.run_context() as s:
        port = s._server.sockets[0].getsockname()[1]

        async def client():
            c = RESPClient("127.0.0.1", port)
            await c.connect()
            return c

        print("=== PING ===")
        c = await client()
        check("PING returns PONG",       await c.send("PING"), "PONG")
        check("PING with arg echoes",    await c.send("PING", "hello"), "hello")
        await c.close()

        print("\n=== Strings ===")
        c = await client()
        check("SET returns OK",          await c.send("SET", "k", "v"), "OK")
        check("GET returns value",       await c.send("GET", "k"), "v")
        check("GET missing = None",      await c.send("GET", "nope"), None)
        check("DEL returns count",       await c.send("DEL", "k"), 1)
        check("GET after DEL = None",    await c.send("GET", "k"), None)
        check("INCR from scratch",       await c.send("INCR", "n"), 1)
        check("INCRBY",                  await c.send("INCRBY", "n", "9"), 10)
        check("EXISTS true",             await c.send("EXISTS", "n"), 1)
        check("EXISTS false",            await c.send("EXISTS", "nope"), 0)
        check("MSET returns OK",         await c.send("MSET", "a", "1", "b", "2"), "OK")
        check("MGET",                    await c.send("MGET", "a", "b"), ["1", "2"])
        await c.close()

        print("\n=== Lists ===")
        c = await client()
        check("RPUSH",    await c.send("RPUSH", "q", "a", "b", "c"), 3)
        check("LRANGE",   await c.send("LRANGE", "q", "0", "-1"), ["a", "b", "c"])
        check("LPOP",     await c.send("LPOP", "q"), "a")
        check("RPOP",     await c.send("RPOP", "q"), "c")
        check("LLEN",     await c.send("LLEN", "q"), 1)
        await c.close()

        print("\n=== Sets ===")
        c = await client()
        check("SADD",            await c.send("SADD", "s", "x", "y", "z"), 3)
        check("SCARD",           await c.send("SCARD", "s"), 3)
        smembers = await c.send("SMEMBERS", "s")
        check("SMEMBERS",        set(smembers), {"x", "y", "z"})
        check("SISMEMBER true",  await c.send("SISMEMBER", "s", "x"), 1)
        check("SISMEMBER false", await c.send("SISMEMBER", "s", "w"), 0)
        check("SREM",            await c.send("SREM", "s", "x"), 1)
        await c.close()

        print("\n=== TTL ===")
        c = await client()
        check("SET EX OK",        await c.send("SET", "tk", "v", "EX", "10"), "OK")
        ttl = await c.send("TTL", "tk")
        check_range("TTL after SET EX", ttl, 9, 10)
        check("PERSIST returns 1", await c.send("PERSIST", "tk"), 1)
        check("TTL after PERSIST", await c.send("TTL", "tk"), -1)
        check("TTL missing key",   await c.send("TTL", "gone"), -2)
        await c.close()

        print("\n=== Pipelining ===")
        c = await client()
        frame = b""
        for cmd in [("SET", "p1", "a"), ("SET", "p2", "b"), ("MGET", "p1", "p2")]:
            frame += f"*{len(cmd)}\r\n".encode()
            for tok in cmd:
                enc = tok.encode()
                frame += f"${len(enc)}\r\n".encode() + enc + b"\r\n"
        c._writer.write(frame)
        await c._writer.drain()
        r1 = await c._read_response()
        r2 = await c._read_response()
        r3 = await c._read_response()
        check("Pipeline SET 1", r1, "OK")
        check("Pipeline SET 2", r2, "OK")
        check("Pipeline MGET",  r3, ["a", "b"])
        await c.close()

        print("\n=== Error handling ===")
        c = await client()
        err = await c.send("UNKNOWNCMD")
        check("Unknown cmd is Exception", isinstance(err, Exception), True)
        await c.send("SET", "strkey", "hello")
        wrongtype = await c.send("LPUSH", "strkey", "x")
        check("WRONGTYPE is Exception", isinstance(wrongtype, Exception), True)
        await c.close()

        print("\n=== Concurrent clients ===")
        clients = [await client() for _ in range(5)]
        for i, cl in enumerate(clients):
            await cl.send("SET", f"cc_{i}", str(i * 10))
        all_ok = True
        for i, cl in enumerate(clients):
            val = await cl.send("GET", f"cc_{i}")
            if val != str(i * 10):
                all_ok = False
        check("Concurrent clients isolated", all_ok, True)
        for cl in clients:
            await cl.close()

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0

if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)