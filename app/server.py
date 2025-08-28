import asyncio
import os
from typing import List, Literal, Optional, TypeAlias, Tuple

from .args_parser import parse_args_to_command
from .commands import (
    DiscardCommand,
    ExecCommand,
    MultiCommand,
    PingCommand,
    PsyncCommand,
    RedisCommand,
    ReplconfCommand,
)
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
            await self._handshake_with_master(master)

        server = await asyncio.start_server(self._client_connected_cb, 'localhost', self._port, reuse_port=True)
        async with server:
            await server.serve_forever()

    @property
    def config(self) -> RedisServerConfig:
        return self._config

    @property
    def replication_id(self) -> str:
        return '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'

    @property
    def replication_offset(self) -> int:
        return 0

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

    async def _handshake_with_master(self, master: RedisConnection) -> None:
        ping = PingCommand()
        await master.write_resp(ping.to_resp_array())
        await master.read_resp()

        replconf1 = ReplconfCommand(args=['listening-port', str(self._port)])
        await master.write_resp(replconf1.to_resp_array())
        await master.read_resp()

        replconf2 = ReplconfCommand(args=['capa', 'psync2'])
        await master.write_resp(replconf2.to_resp_array())
        await master.read_resp()

        psync = PsyncCommand(replication_id='?', offset=-1)
        await master.write_resp(psync.to_resp_array())
        await master.read_resp()

    def _load_databases(self) -> List[RedisDatabase]:
        path = os.path.join(self._config.get('dir'), self._config.get('dbfilename'))
        try:
            return rdb_parse(path)
        except FileNotFoundError:
            return [RedisDatabase() for _ in range(16)]
