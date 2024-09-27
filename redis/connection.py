from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Optional

from .commands import argv_to_command
from .transaction import RedisTransaction

if TYPE_CHECKING:
    from .commands import RedisCommand
    from .server import RedisServer


class RedisConnection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        server: RedisServer,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._server = server
        self._transaction = RedisTransaction(connection=self)

    async def process(self) -> None:
        """Process the connection."""
        while True:
            if (command := await self._recv_command()) is None:
                return
            response = await command.execute(connection=self)
            self._writer.write(response.serialize())
            await self._writer.drain()

    async def close(self) -> None:
        """Close the connection."""
        self._writer.close()
        await self._writer.wait_closed()

    async def _recv_command(self) -> Optional[RedisCommand]:
        """Receive a command from the connection."""
        try:
            encoded_argc = await self._reader.readuntil(b"\r\n")
            argc = int(encoded_argc[1:-2].decode())
            argv = []
            for _ in range(argc):
                encoded_arglen = await self._reader.readuntil(b"\r\n")
                arglen = int(encoded_arglen[1:-2].decode())
                encoded_arg = await self._reader.readexactly(arglen + 2)
                argv.append(encoded_arg[:-2].decode())
            return argv_to_command(argv)
        except asyncio.IncompleteReadError:
            return None

    @property
    def server(self) -> RedisServer:
        """The server processing this connection."""
        return self._server

    @property
    def transaction(self) -> RedisTransaction:
        """The transaction owned by this connection."""
        return self._transaction
