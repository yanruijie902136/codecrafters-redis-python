from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleError, RespSimpleString

if TYPE_CHECKING:
    from ..client import RedisClient


class DiscardCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        if not client.transaction.discard():
            return RespSimpleError("ERR DISCARD without MULTI")
        return RespSimpleString("OK")
