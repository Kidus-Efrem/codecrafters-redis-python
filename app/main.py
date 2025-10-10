import asyncio
from collections import defaultdict, deque

BUF_SIZE = 4096

data = defaultdict(deque)  # key -> deque of elements
blocked_clients = defaultdict(list)  # key -> list of waiting (writer) clients

def encode_bulk_string(s: str) -> bytes:
    return b"$" + str(len(s)).encode() + b"\r\n" + s.encode() + b"\r\n"

def encode_array(arr: list[str]) -> bytes:
    result = b"*" + str(len(arr)).encode() + b"\r\n"
    for item in arr:
        result += encode_bulk_string(item)
    return result

def encode_integer(i: int) -> bytes:
    return b":" + str(i).encode() + b"\r\n"

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    while True:
        try:
            raw = await reader.read(BUF_SIZE)
            if not raw:
                break
            parts = [p.decode() for p in raw.split(b"\r\n") if p and not p.startswith(b"*") and not p.isdigit() and not p.startswith(b"$")]
            if not parts:
                continue

            cmd = parts[0].upper()

            if cmd == "RPUSH":
                key = parts[1]
                vals = parts[2:]
                for v in vals:
                    data[key].append(v)

                # unblock one client waiting for BLPOP on this key
                if key in blocked_clients and blocked_clients[key]:
                    client_writer = blocked_clients[key].pop(0)
                    val = data[key].popleft()
                    resp = encode_array([key, val])
                    client_writer.write(resp)
                    await client_writer.drain()

                writer.write(encode_integer(len(data[key])))
                await writer.drain()

            elif cmd == "LPOP":
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

                if len(popped) == 1:
                    writer.write(encode_bulk_string(popped[0]))
                else:
                    writer.write(encode_array(popped))
                await writer.drain()

            elif cmd == "BLPOP":
                key = parts[1]
                timeout = int(parts[2])
                if data[key]:
                    val = data[key].popleft()
                    writer.write(encode_array([key, val]))
                    await writer.drain()
                else:
                    blocked_clients[key].append(writer)

            elif cmd == "LRANGE":
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
                if start > end:
                    res = []
                else:
                    res = lst[start:end + 1]

                writer.write(encode_array(res))
                await writer.drain()

        except Exception as e:
            print(f"Error: {e}")
            break

async def main():
    server = await asyncio.start_server(handle_command, "127.0.0.1", 6379)
    print("Redis clone running on port 6379...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
