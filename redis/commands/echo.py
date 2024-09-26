from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespBulkString, RespSerializable

if TYPE_CHECKING:
    from ..client import RedisClient


class EchoCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        message = self.argv[1]
        return RespBulkString(message)
