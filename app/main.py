import asyncio

BUF_SIZE = 4096

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        chunk = await reader.read(BUF_SIZE)
        if not chunk:
            break
        i = 0
        if chunk[i] == ord('*'):
            i+=1
            j = chunk.find('\r\n', i)
            arrlen = int(chunk[i:j])
        i = j+2
        elements = []
        for _ in range(arrlen):
            if chunk[i] == ord('$'):
                i+=1
                j = chunk.find('\r\n', i)
                wlen = int(chunk[i:j])
                i = j+2
                element = chunk[i:i+wlen]
                i+= wlen+2
                elements.append(element.decode())
        print(elements)
        if element[0].lower() == 'echo':
            writer.write(b''+elements[1]+'\r\n')
        if element[0].lower() == 'ping':
            writer.write(b''+elements[1]+'\r\n')

        # writer.write(b"+PONG\r\n")
        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
