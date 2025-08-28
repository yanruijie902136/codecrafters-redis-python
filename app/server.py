import asyncio
import os
from typing import List, Literal, Optional, Set, TypeAlias, Tuple

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
        self._master_addr = master_addr
        self._master = None
        self._replicas: Set[RedisConnection] = set()

    def get_database(self, db_index: int) -> RedisDatabase:
        return self._databases[db_index]

    async def run(self) -> None:
        self._databases = self._load_databases()

        server = await asyncio.start_server(self._client_connected_cb, 'localhost', self._port, reuse_port=True)

        async with server, asyncio.TaskGroup() as tg:
            if self._master_addr is not None:
                master_host, master_port = self._master_addr
                reader, writer = await asyncio.open_connection(master_host, master_port)
                self._master = RedisConnection(reader, writer, server=self)
                await self._handshake_with_master()
                tg.create_task(self._handle_connection(self._master))

            tg.create_task(server.serve_forever())

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
        await self._handle_connection(RedisConnection(reader, writer, server=self))

    async def _execute(self, conn: RedisConnection, command: RedisCommand) -> RespValue:
        if conn.transaction.active and not isinstance(command, (DiscardCommand, ExecCommand, MultiCommand)):
            conn.transaction.enqueue(command)
            return RespSimpleString('QUEUED')

        return await command.execute(conn)

    async def _handle_connection(self, conn: RedisConnection) -> None:
        print(f'Accepted connection from {conn.addr}')

        async with conn:
            while True:
                args = await conn.read_args()
                command = parse_args_to_command(args)
                print(f'Received command from {conn.addr}: {command!r}')

                response = await self._execute(conn, command)

                if conn is not self._master:
                    await conn.write_resp(response)

                if command.is_write_command():
                    await self._propagate_command(command)

                if isinstance(command, PsyncCommand):
                    empty_rdb = bytes.fromhex(
                        '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')
                    await conn.write(f'${len(empty_rdb)}\r\n'.encode() + empty_rdb)
                    self._replicas.add(conn)

    async def _handshake_with_master(self) -> None:
        ping = PingCommand()
        await self._master.write_resp(ping.to_resp_array())
        await self._master.read_resp()

        replconf1 = ReplconfCommand(args=['listening-port', str(self._port)])
        await self._master.write_resp(replconf1.to_resp_array())
        await self._master.read_resp()

        replconf2 = ReplconfCommand(args=['capa', 'psync2'])
        await self._master.write_resp(replconf2.to_resp_array())
        await self._master.read_resp()

        psync = PsyncCommand(replication_id='?', offset=-1)
        await self._master.write_resp(psync.to_resp_array())
        await self._master.read_resp()

        await self._master.read_rdb()

    def _load_databases(self) -> List[RedisDatabase]:
        path = os.path.join(self._config.get('dir'), self._config.get('dbfilename'))
        try:
            return rdb_parse(path)
        except FileNotFoundError:
            return [RedisDatabase() for _ in range(16)]

    async def _propagate_command(self, command: RedisCommand) -> None:
        for replica in self._replicas:
            await replica.write_resp(command.to_resp_array())
