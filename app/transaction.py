from typing import TYPE_CHECKING, List

from .protocol import RespArray

if TYPE_CHECKING:
    from .commands import RedisCommand
    from .connection import RedisConnection


class RedisTransaction:
    def __init__(self, conn: 'RedisConnection') -> None:
        self._conn = conn
        self._active = False
        self._commands: List['RedisCommand'] = []

    def enqueue(self, command: 'RedisCommand') -> None:
        if not self._active:
            raise RuntimeError
        self._commands.append(command)

    async def execute(self) -> RespArray:
        if not self._active:
            raise RuntimeError
        responses = [await command.execute(self._conn) for command in self._commands]
        self._active = False
        return RespArray(responses)

    def start(self) -> None:
        if self._active:
            raise RuntimeError
        self._active = True
        self._commands.clear()

    @property
    def active(self) -> bool:
        return self._active
