import asyncio
import time
from collections import defaultdict, deque

BUF_SIZE = 4096

# Key-value store
d = {}  # for set/get with expiration
lst = defaultdict(list)

# Conditions for blocking operations
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
                element = chunk[i:i + wlen]
                i += wlen + 2
                elements.append(element.decode())

        cmd = elements[0].lower()
        # print(f"Command: {elements}")

        if cmd == 'echo':
            writer.write(b"$" + str(len(elements[1])).encode() + b"\r\n" + elements[1].encode() + b"\r\n")

        elif cmd == 'ping':
            writer.write(b"+PONG\r\n")

        elif cmd == 'set':
            addtime = float('inf')
            if len(elements) >= 4:
                addtime = float('inf')
                if len(elements) >= 5:
                    addtime = int(elements[4])
                    if elements[3].upper() == 'PX':
                        addtime /= 1000
                else:
                    addtime = float('inf')

            d[elements[1]] = (elements[2], time.time() + addtime)
            writer.write(b"$2\r\nOK\r\n")

        elif cmd == 'get':
            if elements[1] in d and d[elements[1]][1] >= time.time():
                writer.write(
                    b'$' + str(len(d[elements[1]][0])).encode() + b'\r\n' + d[elements[1]][0].encode() + b'\r\n'
                )
            else:
                writer.write(b"$-1\r\n")

        elif cmd == 'rpush':
            key = elements[1]
            for item in elements[2:]:
                lst[key].append(item)
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')
            async with conditions[key]:
                conditions[key].notify_all()

        elif cmd == 'lpush':
            key = elements[1]
            for item in reversed(elements[2:]):
                lst[key].insert(0, item)
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')

        elif cmd == 'lrange':
            key = elements[1]
            l, r = int(elements[2]), int(elements[3])
            arr = lst[key][l:r + 1] if r >= 0 else lst[key][l:r]
            ans = '*' + str(len(arr))
            for a in arr:
                ans += '\r\n$' + str(len(a)) + '\r\n' + a
            ans += '\r\n'
            writer.write(ans.encode())

        elif cmd == 'llen':
            writer.write(b':' + str(len(lst[elements[1]])).encode() + b'\r\n')

        elif cmd == 'lpop':
            key = elements[1]
            count = int(elements[2]) if len(elements) >= 3 else 1
            popped = lst[key][:count]
            lst[key] = lst[key][count:]
            if popped:
                ans = '*' + str(len(popped))
                for a in popped:
                    ans += '\r\n$' + str(len(a)) + '\r\n' + a
                ans += '\r\n'
                writer.write(ans.encode())
            else:
                writer.write(b'*0\r\n')

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

        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
