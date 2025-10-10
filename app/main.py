import asyncio
import time
from collections import defaultdict, deque
BUF_SIZE = 4096
remove = defaultdict(deque)

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    lst = defaultdict(list)

    conditions = defaultdict(asyncio.condition)
    while True:
        chunk = await reader.read(BUF_SIZE)
        if not chunk:
            break
        i = 0
        if chunk[i] == ord('*'):
            i+=1
            j = chunk.find(b'\r\n', i)
            arrlen = int(chunk[i:j])
        i = j+2
        elements = []
        for _ in range(arrlen):
            if chunk[i] == ord('$'):
                i+=1
                j = chunk.find(b'\r\n', i)
                wlen = int(chunk[i:j])
                i = j+2
                element = chunk[i:i+wlen]
                i+= wlen+2
                elements.append(element.decode())
        print(elements)
        if elements[0].lower() == 'echo':
            writer.write(b"$" + str(len(elements[1])).encode() + b"\r\n" + elements[1].encode() + b"\r\n")

        if elements[0].lower() == 'ping':
            # writer.write(b''++'\r\n')

            writer.write(b"+PONG\r\n")
        if elements[0].lower() == 'set':
            addtime = float('inf')
            if len(elements) >=4:
                addtime = int(elements[4])
                if elements[3] == 'PX':
                    addtime =  int(elements[4])/1000


            d[elements[1]] =(elements[2], time.time()+ addtime)

            writer.write(b"$2\r\nOK\r\n")

        if elements[0].lower() =='get':

            if  elements[1] in d and d[elements[1]][1] >= time.time():
                writer.write(b'$' + str(len(d[elements[1]][0])).encode()+ b'\r\n'+d[elements[1]][0].encode() + b"\r\n")
            else:
                writer.write(b"$-1\r\n")
        if elements[0].lower() == 'rpush':
            i = 2
            while i < len(elements):
                lst[elements[1]].append(elements[i])
                i+=1
            writer.write(b':'+ str(len(lst[elements[1]])).encode()+b'\r\n')
            await writer.drain()
            # writer.write(b'oustide loop'+ remove[elements[1]][-1].encode())

            async with conditions[key]:
                conditions[key].notify_all()
        if elements[0].lower() == 'lpush':
            i = 2
            while i < len(elements):
                lst[elements[1]] = [elements[i]] + lst[elements[1]]
                i+=1
            writer.write(b':'+ str(len(lst[elements[1]])).encode()+b'\r\n')

        if elements[0].lower() == 'lrange':
            i = 2
            l = int(elements[i])
            r = int(elements[i+1])
            arr  =lst[elements[1]][l:r]
            arr+=([lst[elements[1]][r]] if arr and -1*len(lst[elements[1]])< r < len(lst[elements[1]]) else [])
            if arr:
                ans = '*'+ str(len(arr))
                for a in arr:
                    ans += '\r\n'
                    ans += '$'+str(len(a))
                    ans += '\r\n'
                    ans += a
                ans += '\r\n'
                writer.write(ans.encode())
            else:
                writer.write(b'*0\r\n')
        if elements[0].lower() == 'llen':
            writer.write(b':' + str(len(lst[elements[1]])).encode()+ b'\r\n')
        if elements[0].lower() == 'lpop':
            if len(elements) >=3:
                arr  = lst[elements[1]][:int(elements[2])]
                lst[elements[1]] =  lst[elements[1]][int(elements[2]):]
                if arr:
                    ans = '*'+ str(len(arr))
                    for a in arr:
                        ans+='\r\n'
                        ans += '$'+str(len(a))
                        ans += '\r\n'
                        ans += a
                    ans += '\r\n'
                    writer.write(ans.encode())
                else: writer.write(b'*0\r\n')


            elif len(lst[elements[1]]):
                temp  = lst[elements[1]][0]
                lst[elements[1]] =  lst[elements[1]][1:]
                writer.write(b'$'+str(len(temp)).encode()+ b'\r\n' + str(temp).encode()+ b'\r\n')
            else:
                writer.write(b'$-1\r\n')
        if elements[0].lower() == 'blpop':
            key = elements[1]
            timeout = int(elements[2])
            async with conditions[key]:
                if len(lst[key]) > 0:
                    try:
                        await asyncio.wait_for(conditions[key].wait(), timeout)
                    except asyncio.TimeoutError:
                        writer.write(b'$-1\r\n')
                        await writer.drain()
                        continue

                if len(lst[key]) > 0:
                    tmep = lst[key].pop(0)
                    writer.writer(
                    b'*2\r\n'
                    + b'$' + str(len(key)).encode() + b'\r\n' + key.encode() + b'\r\n'
                    + b'$' + str(len(temp)).encode() + b'\r\n' + temp.encode() + b'\r\n'

                    )




        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():

    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
