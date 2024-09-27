from __future__ import annotations
import asyncio
from typing import Optional

from .connection import RedisConnection
from .database import RedisDatabase


class RedisServer:
    def __init__(self, host="localhost", port=6379, **kwargs) -> None:
        self._host, self._port = host, port
        self._config_params: dict[str, Optional[str]] = kwargs
        self._database = RedisDatabase()

    async def start(self) -> None:
        """Start running the server."""
        server = await asyncio.start_server(
            self._process_client, self._host, self._port, reuse_port=True
        )
        async with server:
            await server.serve_forever()

    def get_config_param(self, name: str) -> Optional[str]:
        return self._config_params.get(name)

    async def _process_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Process a client connection. This method is passed as a parameter of
        `asyncio.start_server()`, and is called whenever a new client connection
        is established. The `reader` and `writer` are used to communicate with
        the connection.
        """
        client = RedisConnection(reader, writer, server=self)
        await client.process()
        await client.close()

    @property
    def database(self) -> RedisDatabase:
        """The database owned by this server."""
        return self._database
