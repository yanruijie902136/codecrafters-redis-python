import asyncio
import os
from typing import List, Optional

from .args_parser import parse_args_to_command
from .commands import DiscardCommand, ExecCommand, MultiCommand, RedisCommand
from .connection import RedisConnection
from .database import RedisDatabase, rdb_parse
from .protocol import RespSimpleString, RespValue


class RedisServerConfig:
    def __init__(self, dbfilename: str = 'dump.rdb', dir: str = './') -> None:
        self._params = {
            'dbfilename': dbfilename,
            'dir': dir,
        }

    def get(self, param: str) -> Optional[str]:
        return self._params.get(param)


class RedisServer:
    def __init__(self, config: RedisServerConfig) -> None:
        self._config = config
        self._databases = self._load_databases()

    def get_database(self, db_index: int) -> RedisDatabase:
        return self._databases[db_index]

    async def run(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, 'localhost', 6379, reuse_port=True)
        async with server:
            await server.serve_forever()

    @property
    def config(self) -> RedisServerConfig:
        return self._config

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = RedisConnection(reader, writer, server=self)
        print(f'Accepted connection from {conn.addr}')

        async with conn:
            while True:
                args = await conn.read_args()
                command = parse_args_to_command(args)
                print(f'Received command from {conn.addr}: {command!r}')

                response = await self._execute(conn, command)
                await conn.write_response(response)

    async def _execute(self, conn: RedisConnection, command: RedisCommand) -> RespValue:
        if conn.transaction.active and not isinstance(command, (DiscardCommand, ExecCommand, MultiCommand)):
            conn.transaction.enqueue(command)
            return RespSimpleString('QUEUED')

        return await command.execute(conn)

    def _load_databases(self) -> List[RedisDatabase]:
        path = os.path.join(self._config.get('dir'), self._config.get('dbfilename'))
        try:
            return rdb_parse(path)
        except FileNotFoundError:
            return [RedisDatabase() for _ in range(16)]
