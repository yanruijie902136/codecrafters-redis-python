import asyncio

from .client import RedisClient
from .database import RedisDatabase


class RedisServer:
    def __init__(self, host="localhost", port=6379) -> None:
        self._host, self._port = host, port
        self._database = RedisDatabase()

    async def start(self) -> None:
        server = await asyncio.start_server(
            self._handle_client, self._host, self._port, reuse_port=True
        )
        async with server:
            await server.serve_forever()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        client = RedisClient(reader, writer, server=self)
        await client.process()
        await client.close()

    @property
    def database(self) -> RedisDatabase:
        return self._database
