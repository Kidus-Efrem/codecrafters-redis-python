import asyncio
import time
from collections import defaultdict
BUF_SIZE = 4096

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    lst = []
    d = defaultdict(str)
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
            lst.append(elements[1])
            writer.write(b'$1\r\n'+ str(len(lst)).encode()+b'\r\n')
        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
