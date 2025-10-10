import asyncio
import time
from collections import defaultdict, deque

BUF_SIZE = 4096

# Data stores
d = {}  # key -> (value, expire_time)
lst = defaultdict(list)  # key -> list
conditions = defaultdict(asyncio.Condition)

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        chunk = await reader.read(BUF_SIZE)
        if not chunk:
            break

        i = 0
        if chunk[i] == ord('*'):
            i += 1
            j = chunk.find(b'\r\n', i)
            arrlen = int(chunk[i:j])
            i = j + 2

        elements = []
        for _ in range(arrlen):
            if chunk[i] == ord('$'):
                i += 1
                j = chunk.find(b'\r\n', i)
                wlen = int(chunk[i:j])
                i = j + 2
                element = chunk[i:i+wlen]
                i += wlen + 2
                elements.append(element.decode())

        cmd = elements[0].lower()

        # ECHO
        if cmd == 'echo':
            writer.write(b"$" + str(len(elements[1])).encode() + b"\r\n" + elements[1].encode() + b"\r\n")

        # PING
        elif cmd == 'ping':
            writer.write(b"+PONG\r\n")

        # SET
        elif cmd == 'set':
            addtime = float('inf')
            if len(elements) >= 4:
                if len(elements) >= 5:
                    addtime = int(elements[4])
                    if elements[3].upper() == 'PX':
                        addtime = addtime / 1000
            d[elements[1]] = (elements[2], time.time() + addtime)
            writer.write(b"$2\r\nOK\r\n")

        # GET
        elif cmd == 'get':
            if elements[1] in d and d[elements[1]][1] >= time.time():
                writer.write(b'$' + str(len(d[elements[1]][0])).encode() + b'\r\n' + d[elements[1]][0].encode() + b'\r\n')
            else:
                writer.write(b"$-1\r\n")

        # RPUSH
        elif cmd == 'rpush':
            key = elements[1]
            for v in elements[2:]:
                lst[key].append(v)
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')

            # Notify any waiting BLPOP
            async with conditions[key]:
                conditions[key].notify_all()

        # LPUSH
        elif cmd == 'lpush':
            key = elements[1]
            for v in reversed(elements[2:]):
                lst[key] = [v] + lst[key]
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')

        # LLEN
        elif cmd == 'llen':
            writer.write(b':' + str(len(lst[elements[1]])).encode() + b'\r\n')

        # LPOP
        elif cmd == 'lpop':
            key = elements[1]
            count = int(elements[2]) if len(elements) >= 3 else 1

            if lst[key]:
                arr = lst[key][:count]
                lst[key] = lst[key][count:]
                ans = '*' + str(len(arr))
                for a in arr:
                    ans += '\r\n$' + str(len(a)) + '\r\n' + a
                ans += '\r\n'
                writer.write(ans.encode())
            else:
                writer.write(b'*0\r\n')

        # BLPOP
        elif cmd == 'blpop':
            key = elements[1]
            timeout = int(elements[2])
            async with conditions[key]:
                end_time = None if timeout == 0 else time.time() + timeout
                while not lst[key]:
                    if timeout != 0:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            break
                    try:
                        await asyncio.wait_for(conditions[key].wait(), remaining if timeout != 0 else None)
                    except asyncio.TimeoutError:
                        break

                if lst[key]:
                    temp = lst[key].pop(0)
                    writer.write(
                        b'*2\r\n'
                        + b'$' + str(len(key)).encode() + b'\r\n' + key.encode() + b'\r\n'
                        + b'$' + str(len(temp)).encode() + b'\r\n' + temp.encode() + b'\r\n'
                    )
                else:
                    writer.write(b'$-1\r\n')

        # LRANGE
        elif cmd == 'lrange':
            key = elements[1]
            l = int(elements[2])
            r = int(elements[3])
            lst_len = len(lst[key])

            # Handle negative indices
            if l < 0:
                l += lst_len
            if r < 0:
                r += lst_len

            # Clamp
            l = max(l, 0)
            r = min(r, lst_len - 1)

            if l > r or lst_len == 0:
                arr = []
            else:
                arr = lst[key][l:r + 1]

            ans = '*' + str(len(arr))
            for a in arr:
                ans += '\r\n$' + str(len(a)) + '\r\n' + a
            ans += '\r\n'
            writer.write(ans.encode())

        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
