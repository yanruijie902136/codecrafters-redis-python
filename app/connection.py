import asyncio
from types import TracebackType
from typing import List, Optional, Self, Tuple, Type

from .protocol import RespValue, resp_decode


class RedisConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

        self._host, self._port, *_ = writer.get_extra_info('peername')

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def read_args(self) -> List[bytes]:
        args = (await resp_decode(self._reader)).to_builtin()

        if not isinstance(args, list) or not all(isinstance(arg, bytes) for arg in args):
            raise RuntimeError('Command arguments must be sent as an array of bulk strings')

        return args

    async def write_response(self, response: RespValue) -> None:
        self._writer.write(response.encode())
        await self._writer.drain()

    @property
    def addr(self) -> Tuple[str, int]:
        return self._host, self._port

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        await self.close()

        if exc_val is None:
            print(f'Closed connection from {self.addr} normally')
            return None

        print(f'Closed connection from {self.addr} due to {exc_val!r}')
        return isinstance(exc_val, asyncio.IncompleteReadError)
