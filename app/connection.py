import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, List, Optional, Self, Set, Tuple, Type

from .database import RedisDatabase
from .protocol import RespValue, resp_decode
from .transaction import RedisTransaction

if TYPE_CHECKING:
    from .server import RedisServer


class RedisConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: 'RedisServer') -> None:
        self._reader = reader
        self._writer = writer
        self._server = server

        self._host, self._port, *_ = writer.get_extra_info('peername')
        self._transaction = RedisTransaction(conn=self)

        self._subbed_channels: Set[str] = set()

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def read_args(self) -> List[bytes]:
        args = (await self.read_resp()).to_builtin()

        if not isinstance(args, list) or not all(isinstance(arg, bytes) for arg in args):
            raise RuntimeError('Command arguments must be sent as an array of bulk strings')

        return args

    async def read_rdb(self) -> None:
        assert await self._reader.readexactly(1) == b'$'
        length = int(await self._reader.readuntil(b'\r\n'))
        await self._reader.readexactly(length)

    async def read_resp(self) -> RespValue:
        return await resp_decode(self._reader)

    def subscribe(self, channel: str) -> None:
        self._subbed_channels.add(channel)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def write_resp(self, value: RespValue) -> None:
        await self.write(value.encode())

    @property
    def addr(self) -> Tuple[str, int]:
        return self._host, self._port

    @property
    def database(self) -> RedisDatabase:
        return self._server.get_database(0)

    @property
    def has_subbed(self) -> bool:
        return self.num_subbed_channels > 0

    @property
    def num_subbed_channels(self) -> int:
        return len(self._subbed_channels)

    @property
    def server(self) -> 'RedisServer':
        return self._server

    @property
    def transaction(self) -> RedisTransaction:
        return self._transaction

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> Optional[bool]:
        await self.close()

        if exc_val is None:
            print(f'Closed connection from {self.addr} normally')
            return None

        print(f'Closed connection from {self.addr} due to {exc_val!r}')
        return isinstance(exc_val, asyncio.IncompleteReadError)
