from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleString

if TYPE_CHECKING:
    from ..client import RedisClient


class PingCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        return RespSimpleString("PONG")
