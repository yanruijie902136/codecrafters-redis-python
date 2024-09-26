from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Optional

from .commands import RedisCommand, argv_to_command
from .resp2 import RespSimpleString
from .transaction import RedisTransaction

if TYPE_CHECKING:
    from .server import RedisServer


class RedisClient:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        server: RedisServer,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._server = server
        self._transaction = RedisTransaction(client=self)

    async def process(self) -> None:
        while True:
            if (command := await self._recv_command()) is None:
                return

            if self._transaction.queue(command):
                response = RespSimpleString("QUEUED")
            else:
                response = command.execute(client=self)
            self._writer.write(response.serialize())
            await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def _recv_command(self) -> Optional[RedisCommand]:
        try:
            encoded_argc = await self._reader.readuntil()
            argc = int(encoded_argc[1:-2].decode())
            argv = []
            for _ in range(argc):
                encoded_argsize = await self._reader.readuntil()
                argsize = int(encoded_argsize[1:-2].decode())
                encoded_arg = await self._reader.readexactly(argsize + 2)
                argv.append(encoded_arg[:-2].decode())

            return argv_to_command(argv)
        except asyncio.IncompleteReadError:
            return None

    @property
    def server(self) -> RedisServer:
        return self._server

    @property
    def transaction(self) -> RedisTransaction:
        return self._transaction
