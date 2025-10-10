import asyncio
from collections import defaultdict, deque

data = defaultdict(deque)
blocked_clients = defaultdict(list)

# --- RESP Encoding Helpers ---
def encode_bulk(value: str) -> bytes:
    if value is None:
        return b"$-1\r\n"
    return b"$" + str(len(value)).encode() + b"\r\n" + value.encode() + b"\r\n"

def encode_array(values: list[str]) -> bytes:
    out = b"*" + str(len(values)).encode() + b"\r\n"
    for v in values:
        out += encode_bulk(v)
    return out

def encode_integer(num: int) -> bytes:
    return b":" + str(num).encode() + b"\r\n"

# --- Core Logic ---
async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                count = int(line.strip()[1:])
            except Exception:
                continue
            parts = []
            for _ in range(count):
                length_line = await reader.readline()
                length = int(length_line.strip()[1:])
                arg = (await reader.readexactly(length + 2))[:-2].decode()
                parts.append(arg)

            cmd = parts[0].upper()

            # --- RPUSH ---
            if cmd == "RPUSH":
                if len(parts) < 3:
                    continue
                key = parts[1]
                vals = parts[2:]
                for v in vals:
                    data[key].append(v)

                # ✅ Get length BEFORE unblocking
                list_len = len(data[key])

                # ✅ Unblock a waiting BLPOP client if present
                if key in blocked_clients and blocked_clients[key]:
                    waiting_writer = blocked_clients[key].pop(0)
                    val = data[key].popleft()
                    resp = encode_array([key, val])
                    waiting_writer.write(resp)
                    await waiting_writer.drain()

                # ✅ Return correct list length
                writer.write(encode_integer(list_len))
                await writer.drain()

            # --- LPOP ---
            elif cmd == "LPOP":
                key = parts[1]
                count = int(parts[2]) if len(parts) > 2 else 1
                result = []
                for _ in range(count):
                    if not data[key]:
                        break
                    result.append(data[key].popleft())

                if len(result) == 0:
                    writer.write(b"$-1\r\n")
                elif len(result) == 1 and (len(parts) == 2):
                    # Single element → bulk string
                    writer.write(encode_bulk(result[0]))
                else:
                    # Multiple elements → array
                    writer.write(encode_array(result))
                await writer.drain()

            # --- BLPOP ---
            elif cmd == "BLPOP":
                key = parts[1]
                timeout = int(parts[2])
                if data[key]:
                    val = data[key].popleft()
                    writer.write(encode_array([key, val]))
                    await writer.drain()
                else:
                    # Block client
                    blocked_clients[key].append(writer)

            else:
                # Unsupported command
                writer.write(b"-ERR unknown command\r\n")
                await writer.drain()

    except Exception as e:
        print("[your_program] Error:", e)
    finally:
        writer.close()
        await writer.wait_closed()

# --- Main ---
async def main():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 6379)
    print("[your_program] Redis clone running on port 6379...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
