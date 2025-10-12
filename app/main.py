import asyncio
import time
from collections import defaultdict, deque


# Global shared state
lst = defaultdict(list)         # The actual Redis-like lists (key -> list of strings)
waiting = defaultdict(deque)    # Keys that have BLPOP clients waiting (key -> deque of (writer, expiry))


async def handle_command(reader, writer):
    addr = writer.get_extra_info('peername')
    while True:
        try:
            # Read the RESP command
            data = await reader.readline()
            if not data:
                break

            if data.startswith(b'*'):
                num_args = int(data[1:-2])
                elements = []
                for _ in range(num_args):
                    await reader.readline()               # skip length line
                    bulk = await reader.readline()
                    elements.append(bulk.strip().decode())

                command = elements[0].lower()

                # --- RPUSH command ---
                if command == 'rpush':
                    key = elements[1]
                    # append all given values to the list
                    for value in elements[2:]:
                        lst[key].append(value)

                    # Reply to RPUSH caller: length of the list
                    writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')
                    await writer.drain()

                    # If someone is waiting on BLPOP for this key, wake them
                    if waiting[key]:
                        blocked_writer, expiry = waiting[key].popleft()
                        # deliver one element to the blocked client
                        if lst[key]:
                            value = lst[key].pop(0)
                            try:
                                blocked_writer.write(
                                    b'*2\r\n'
                                    + b'$' + str(len(key)).encode() + b'\r\n' + key.encode() + b'\r\n'
                                    + b'$' + str(len(value)).encode() + b'\r\n' + value.encode() + b'\r\n'
                                )
                                await blocked_writer.drain()
                            except Exception:
                                pass  # client disconnected, ignore

                # --- BLPOP command ---
                elif command == 'blpop':
                    key = elements[1]
                    timeout = float(elements[2])

                    # immediate pop if available
                    if lst[key]:
                        value = lst[key].pop(0)
                        writer.write(
                            b'*2\r\n'
                            + b'$' + str(len(key)).encode() + b'\r\n' + key.encode() + b'\r\n'
                            + b'$' + str(len(value)).encode() + b'\r\n' + value.encode() + b'\r\n'
                        )
                        await writer.drain()
                    else:
                        # store waiting client and schedule timeout if > 0
                        expiry = (time.time() + timeout) if timeout > 0 else 0
                        waiting[key].append((writer, expiry))

                        if timeout > 0:
                            async def timeout_task(wr, k, exp):
                                await asyncio.sleep(timeout)
                                # still waiting?
                                for (w, e) in list(waiting[k]):
                                    if w is wr:
                                        waiting[k].remove((w, e))
                                        try:
                                            w.write(b"$-1\r\n")
                                            await w.drain()
                                        except Exception:
                                            pass
                                        break
                            asyncio.create_task(timeout_task(writer, key, expiry))

                # --- LRANGE (optional for debugging) ---
                elif command == 'lrange':
                    key = elements[1]
                    start, end = int(elements[2]), int(elements[3])
                    arr = lst[key][start:end + 1]
                    reply = b'*' + str(len(arr)).encode() + b'\r\n'
                    for item in arr:
                        reply += b'$' + str(len(item)).encode() + b'\r\n' + item.encode() + b'\r\n'
                    writer.write(reply)
                    await writer.drain()

                else:
                    writer.write(b"-ERR unknown command\r\n")
                    await writer.drain()

        except Exception as e:
            print(f"Error with client {addr}: {e}")
            break

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_command, '127.0.0.1', 6379)
    addr = server.sockets[0].getsockname()
    print(f"Async Redis clone running on {addr}")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
