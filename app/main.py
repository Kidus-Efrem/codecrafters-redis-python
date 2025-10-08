import asyncio

BUF_SIZE = 4096

async def handle_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        chunk = await reader.read(BUF_SIZE)
        if not chunk:
            break
        writer.write(b"+PONG\r\n")
        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_command, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
