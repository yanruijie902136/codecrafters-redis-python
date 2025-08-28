import asyncio
import os
from typing import List, Literal, Optional, TypeAlias, Tuple

from .args_parser import parse_args_to_command
from .commands import DiscardCommand, ExecCommand, MultiCommand, RedisCommand
from .connection import RedisConnection
from .database import RedisDatabase, rdb_parse
from .protocol import *


class RedisServerConfig:
    def __init__(self, dbfilename: str = 'dump.rdb', dir: str = './') -> None:
        self._params = {
            'dbfilename': dbfilename,
            'dir': dir,
        }

    def get(self, param: str) -> Optional[str]:
        return self._params.get(param)


_Address: TypeAlias = Tuple[str, int]


class RedisServer:
    def __init__(self, port: int, config: RedisServerConfig, master_addr: Optional[_Address] = None) -> None:
        self._port = port
        self._config = config
        self._databases = self._load_databases()
        self._master_addr = master_addr

    def get_database(self, db_index: int) -> RedisDatabase:
        return self._databases[db_index]

    async def run(self) -> None:
        if self._master_addr is not None:
            master_host, master_port = self._master_addr
            reader, writer = await asyncio.open_connection(master_host, master_port)
            master = RedisConnection(reader, writer, server=self)
            await master.write_resp(RespArray([RespBulkString('PING')]))

        server = await asyncio.start_server(self._client_connected_cb, 'localhost', self._port, reuse_port=True)
        async with server:
            await server.serve_forever()

    @property
    def config(self) -> RedisServerConfig:
        return self._config

    @property
    def role(self) -> Literal['master', 'slave']:
        return 'master' if self._master_addr is None else 'slave'

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = RedisConnection(reader, writer, server=self)
        print(f'Accepted connection from {conn.addr}')

        async with conn:
            while True:
                args = await conn.read_args()
                command = parse_args_to_command(args)
                print(f'Received command from {conn.addr}: {command!r}')

                response = await self._execute(conn, command)
                await conn.write_resp(response)

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
