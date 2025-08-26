import asyncio
from types import TracebackType
from typing import Optional, Self, Type


class RedisConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def read(self) -> bytes:
        return await self._reader.read(4096)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        await self.close()
