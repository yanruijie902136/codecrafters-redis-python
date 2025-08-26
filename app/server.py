import asyncio

from .connection import RedisConnection


class RedisServer:
    async def run(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, 'localhost', 6379, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with RedisConnection(reader, writer) as conn:
            while True:
                data = await conn.read()
                if not data:
                    break
                await conn.write(b'+PONG\r\n')
