import asyncio
from collections import defaultdict, deque

data = defaultdict(deque)
blocked_clients = defaultdict(list)
BUF_SIZE = 4096

# ---------------- RESP ENCODING HELPERS ----------------
def encode_bulk_string(s: str) -> bytes:
    return b"$" + str(len(s)).encode() + b"\r\n" + s.encode() + b"\r\n"

def encode_array(arr: list[str]) -> bytes:
    res = b"*" + str(len(arr)).encode() + b"\r\n"
    for a in arr:
        res += encode_bulk_string(a)
    return res

def encode_integer(i: int) -> bytes:
    return b":" + str(i).encode() + b"\r\n"

# ---------------- RESP PARSING ----------------
def parse_resp(raw: bytes):
    """Simplified RESP parser that safely extracts command and args."""
    lines = [l for l in raw.split(b"\r\n") if l]
    arr = []
    for line in lines:
        if line.startswith(b"*") or line.startswith(b"$"):
            continue
        arr.append(line.decode())
    return arr

# ---------------- MAIN HANDLER ----------------
async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    while True:
        try:
            raw = await reader.read(BUF_SIZE)
            if not raw:
                break

            parts = parse_resp(raw)
            if not parts:
                continue

            cmd = parts[0].upper()

            # RPUSH key value [value ...]
            if cmd == "RPUSH":
                if len(parts) < 3:
                    continue
                key = parts[1]
                vals = parts[2:]
                for v in vals:
                    data[key].append(v)

                # Unblock one waiting BLPOP client
                if key in blocked_clients and blocked_clients[key]:
                    waiting_writer = blocked_clients[key].pop(0)
                    val = data[key].popleft()
                    resp = encode_array([key, val])
                    waiting_writer.write(resp)
                    await waiting_writer.drain()

                writer.write(encode_integer(len(data[key])))
                await writer.drain()

            # LPOP key [count]
            elif cmd == "LPOP":
                if len(parts) < 2:
                    continue
                key = parts[1]
                count = 1
                if len(parts) > 2:
                    try:
                        count = int(parts[2])
                    except:
                        pass

                popped = []
                for _ in range(count):
                    if data[key]:
                        popped.append(data[key].popleft())
                    else:
                        break

                if not popped:
                    writer.write(b"$-1\r\n")
                elif len(popped) == 1:
                    writer.write(encode_bulk_string(popped[0]))
                else:
                    writer.write(encode_array(popped))
                await writer.drain()

            # BLPOP key timeout
            elif cmd == "BLPOP":
                if len(parts) < 3:
                    continue
                key = parts[1]
                timeout = int(parts[2])
                if data[key]:
                    val = data[key].popleft()
                    writer.write(encode_array([key, val]))
                    await writer.drain()
                else:
                    # Add to blocking queue
                    blocked_clients[key].append(writer)
                    # Don't respond yet — RPUSH will handle

            # LRANGE key start end
            elif cmd == "LRANGE":
                if len(parts) < 4:
                    continue
                key = parts[1]
                start = int(parts[2])
                end = int(parts[3])
                lst = list(data[key])

                n = len(lst)
                if end == -1:
                    end = n - 1
                if start < 0:
                    start = n + start
                if end < 0:
                    end = n + end
                start = max(0, start)
                end = min(n - 1, end)
                if start > end or n == 0:
                    res = []
                else:
                    res = lst[start:end + 1]

                writer.write(encode_array(res))
                await writer.drain()

        except Exception as e:
            print(f"[{addr}] Error: {e}")
            break

# ---------------- SERVER ENTRYPOINT ----------------
async def main():
    server = await asyncio.start_server(handle_command, "127.0.0.1", 6379)
    print("Redis clone running on port 6379...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
