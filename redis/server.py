from __future__ import annotations
import asyncio
import os
from typing import Literal, Optional

from .connection import RedisConnection
from .database import RedisDatabase, RedisDatabaseLoader


Address = tuple[str, int]


class RedisServer:
    def __init__(
        self, address: Address, master_address: Optional[Address] = None, **kwargs
    ) -> None:
        self._address = address
        self._master_address = master_address

        self._config_params: dict[str, Optional[str]] = kwargs
        self._database = self._try_load_database()

    async def start(self) -> None:
        """Start running the server."""
        server = await asyncio.start_server(self._process_client, *self._address, reuse_port=True)
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

    def _try_load_database(self) -> RedisDatabase:
        """
        Try to load the database from an existing RDB file. If the operation
        fails, the server starts with an empty database.
        """
        try:
            rdb_filepath = os.path.join(
                self._config_params["dir"], self._config_params["dbfilename"]
            )
            return RedisDatabaseLoader().load(rdb_filepath)
        except:
            return RedisDatabase()

    @property
    def database(self) -> RedisDatabase:
        """The database owned by this server."""
        return self._database

    @property
    def role(self) -> Literal["master", "slave"]:
        """The server's role."""
        return "master" if self._master_address is None else "slave"
