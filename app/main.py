import asyncio
import time
from collections import defaultdict, deque

BUF_SIZE = 4096
lst = defaultdict(list)
d = defaultdict(tuple)
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

        if not elements:
            continue

        cmd = elements[0].lower()

        if cmd == 'echo' and len(elements) >= 2:
            writer.write(b"$" + str(len(elements[1])).encode() + b"\r\n" + elements[1].encode() + b"\r\n")

        elif cmd == 'ping':
            writer.write(b"+PONG\r\n")

        elif cmd == 'set' and len(elements) >= 3:
            addtime = float('inf')
            if len(elements) >= 5 and elements[3].upper() == 'PX':
                addtime = int(elements[4]) / 1000
            elif len(elements) >= 5:
                addtime = int(elements[4])
            d[elements[1]] = (elements[2], time.time() + addtime)
            writer.write(b"$2\r\nOK\r\n")

        elif cmd == 'get' and len(elements) >= 2:
            key = elements[1]
            if key in d and d[key][1] >= time.time():
                val = d[key][0]
                writer.write(b'$' + str(len(val)).encode() + b'\r\n' + val.encode() + b'\r\n')
            else:
                writer.write(b"$-1\r\n")

        elif cmd == 'rpush' and len(elements) >= 3:
            key = elements[1]
            for val in elements[2:]:
                lst[key].append(val)
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')
            async with conditions[key]:
                conditions[key].notify_all()

        elif cmd == 'lpush' and len(elements) >= 3:
            key = elements[1]
            for val in reversed(elements[2:]):
                lst[key].insert(0, val)
            writer.write(b':' + str(len(lst[key])).encode() + b'\r\n')

        elif cmd == 'lrange' and len(elements) >= 4:
            key = elements[1]
            l = int(elements[2])
            r = int(elements[3])
            arr = lst[key][l:r + 1] if r >= 0 else lst[key][l:r + 1]
            if arr:
                ans = '*' + str(len(arr))
                for a in arr:
                    ans += '\r\n$' + str(len(a)) + '\r\n' + a
                ans += '\r\n'
                writer.write(ans.encode())
            else:
                writer.write(b'*0\r\n')

        elif cmd == 'llen' and len(elements) >= 2:
            writer.write(b':' + str(len(lst[elements[1]])).encode() + b'\r\n')

        elif cmd == 'lpop' and len(elements) >= 2:
            key = elements[1]
            count = int(elements[2]) if len(elements) >= 3 else 1
            if not lst[key]:
                writer.write(b'$-1\r\n')
            else:
                if count == 1:
                    temp = lst[key].pop(0)
                    writer.write(b'$' + str(len(temp)).encode() + b'\r\n' + temp.encode() + b'\r\n')
                else:
                    arr = lst[key][:count]
                    lst[key] = lst[key][count:]
                    ans = '*' + str(len(arr))
                    for a in arr:
                        ans += '\r\n$' + str(len(a)) + '\r\n' + a
                    ans += '\r\n'
                    writer.write(ans.encode())

        elif cmd == 'blpop' and len(elements) >= 3:
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
