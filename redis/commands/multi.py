from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleError, RespSimpleString

if TYPE_CHECKING:
    from ..client import RedisClient


class MultiCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        if not client.transaction.start():
            return RespSimpleError("ERR MULTI calls can not be nested")
        return RespSimpleString("OK")
