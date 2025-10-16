import asyncio
import time
from collections import defaultdict, deque
# from sortedcontainers import SortedDict

BUF_SIZE = 4096

# Global data stores
lst = defaultdict(list)      # For Redis lists
remove = defaultdict(deque)  # For blocked clients (key → deque of writers)
d = defaultdict(tuple)       # For key-value store with expiry
streams= defaultdict(lambda:defaultdict(list))
# streams = set()
lastusedtime = 0
lastusedseq = defaultdict(int)

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global lst, remove, d

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

        print(elements)
        cmd = elements[0].lower()

        # ---------------- PING ----------------
        if cmd == 'ping':
            writer.write(b"+PONG\r\n")

        # ---------------- ECHO ----------------
        elif cmd == 'echo':
            msg = elements[1]
            writer.write(b"$" + str(len(msg)).encode() + b"\r\n" + msg.encode() + b"\r\n")

        # ---------------- SET ----------------
        elif cmd == 'set':
            key, value = elements[1], elements[2]
            expiry = float('inf')

            if len(elements) >= 5 and elements[3].upper() == 'PX':
                expiry = time.time() + int(elements[4]) / 1000

            d[key] = (value, expiry)
            writer.write(b"+OK\r\n")

        # ---------------- GET ----------------
        elif cmd == 'get':
            key = elements[1]
            if key in d:
                val, expiry = d[key]
                if expiry < time.time():
                    del d[key]
                    writer.write(b"$-1\r\n")
                else:
                    writer.write(f"${len(val)}\r\n{val}\r\n".encode())
            else:
                writer.write(b"$-1\r\n")

        # ---------------- RPUSH ----------------
        elif cmd == 'rpush':
            key = elements[1]
            for v in elements[2:]:
                lst[key].append(v)
            writer.write(f":{len(lst[key])}\r\n".encode())

            # Wake up first blocked client (if any)
            if key in remove and remove[key]:
                blocked_writer = remove[key].popleft()
                if lst[key]:
                    element = lst[key].pop(0)
                    resp = (
                        f"*2\r\n${len(key)}\r\n{key}\r\n"
                        f"${len(element)}\r\n{element}\r\n"
                    ).encode()
                    blocked_writer.write(resp)
                    await blocked_writer.drain()

        # ---------------- LPUSH ----------------
        elif cmd == 'lpush':
            key = elements[1]
            for v in elements[2:]:
                lst[key].insert(0, v)
            writer.write(f":{len(lst[key])}\r\n".encode())

        # ---------------- LLEN ----------------
        elif cmd == 'llen':
            key = elements[1]
            writer.write(f":{len(lst[key])}\r\n".encode())

        # ---------------- LRANGE ----------------
        elif cmd == 'lrange':
            key = elements[1]
            start = int(elements[2])
            end = int(elements[3])
            arr = lst[key]
            n = len(arr)

            if start < 0:
                start += n
            if end < 0:
                end += n
            start = max(0, start)
            end = min(end, n - 1)
            if start > end or n == 0:
                writer.write(b"*0\r\n")
            else:
                subset = arr[start:end + 1]
                resp = f"*{len(subset)}\r\n".encode()
                for item in subset:
                    resp += f"${len(item)}\r\n{item}\r\n".encode()
                writer.write(resp)

        # ---------------- LPOP ----------------
        elif cmd == 'lpop':
            key = elements[1]
            if len(elements) == 2:
                if lst[key]:
                    element = lst[key].pop(0)
                    writer.write(f"${len(element)}\r\n{element}\r\n".encode())
                else:
                    writer.write(b"$-1\r\n")
            else:
                count = int(elements[2])
                popped = [lst[key].pop(0) for _ in range(min(count, len(lst[key])))]
                if popped:
                    resp = f"*{len(popped)}\r\n".encode()
                    for item in popped:
                        resp += f"${len(item)}\r\n{item}\r\n".encode()
                    writer.write(resp)
                else:
                    writer.write(b"*0\r\n")

        # ---------------- BLPOP ----------------
        elif cmd == 'blpop':
            key = elements[1]
            timeout = float(elements[2])

            # If the list has items, return immediately
            if lst[key]:
                element = lst[key].pop(0)
                resp = (
                    f"*2\r\n${len(key)}\r\n{key}\r\n"
                    f"${len(element)}\r\n{element}\r\n"
                ).encode()
                writer.write(resp)
                await writer.drain()
            else:
                # No elements → block
                remove[key].append(writer)

                async def unblock_after_timeout():
                    if timeout != 0:
                        await asyncio.sleep(timeout)
                        # Still blocked? Unblock with nil
                        if writer in remove[key]:
                            remove[key].remove(writer)
                            writer.write(b"*-1\r\n")
                            await writer.drain()

                asyncio.create_task(unblock_after_timeout())
        elif cmd == 'type':
            key = elements[1]
            if key in streams:
                writer.write(b"+stream\r\n")

            elif key in d and d[key][1] >= time.time():
                val = d[key][0]
                if isinstance(val, str):

                    typename = "string"
                elif isinstance(val, list):
                    typename = "list"
                else:
                    typename = "none"
                writer.write(f"+{typename}\r\n".encode())
            else:
                writer.write(b"+none\r\n")

        elif cmd == 'xadd':
            global lastusedtime
            if len(elements[2]) == 1:
                t = time.time_ns()//1000000
                if t in lastusedseq:
                    sequence = lastusedseq[t] +1
                else:
                    sequence = 0
            else:
                t, sequence = elements[2].split('-')
                t = int(t)

                if sequence == "*":
                    if t in lastusedseq:
                        sequence  = lastusedseq[t]+1

                    else:
                        sequence = 0
                        if t == 0:
                            sequence +=1

                sequence  = int(sequence)
            if sequence == '*':
                writer.write(b"hell no")
            elif t == sequence and t == 0:
                writer.write(b'-ERR The ID specified in XADD must be greater than 0-0\r\n')
            elif t < lastusedtime:
                writer.write(b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n')

            elif t == lastusedtime and  sequence<=lastusedseq[lastusedtime] :
                writer.write(b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n')

            else:
                lastusedtime = t
                lastusedseq[lastusedtime] = sequence
                # streams[elements[1]]
                streams[elements[1]][elements[2]].append([elements[3],elements[4]])
                id = elements[2]
                if elements[2] == '*':
                    writer.write(b'$' + str(len(str(sequence)) +1+ len(str(t))).encode()+b'\r\n' +f'{t}-{sequence}\r\n'.encode())
                writer.write(f'+{t}-{sequence}\r\n'.encode())

        elif cmd == 'xrange':
            # pass
            start, end = elements[2], elements[3]
            key = elements[1]
            ans = ''
            cnt = 0
            for k, v in streams[key].items():
                if start<=k<=end:
                    ans+="*2\r\n"
                    ans +='$' + str(len(k))+"\r\n" +k+"\r\n"
                    cnt +=1
                    local = 0
                    ans +='*'+str(len(v)*2)+'\r\n'
                    for a, b in v:
                        ans+='$'+ str(len(a))+'\r\n'+a+'\r\n'
                        ans+='$'+ str(len(b))+'\r\n'+b+'\r\n'
            writer.write(ans.encode())


        else:
            writer.write(b"-ERR unknown command\r\n")

        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    print("Async Redis clone running on ('127.0.0.1', 6379)")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
