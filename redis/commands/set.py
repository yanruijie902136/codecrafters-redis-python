from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleString

if TYPE_CHECKING:
    from ..client import RedisClient


class SetCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        key, value = self.argv[1], self.argv[2]
        if len(self.argv) == 3:
            client.server.database.set(key, value)
        else:
            expire_time = float(self.argv[4])
            client.server.database.set(key, value, expire_time=expire_time)
        return RespSimpleString("OK")
