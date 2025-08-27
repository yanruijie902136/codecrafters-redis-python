__all__ = 'XaddCommand',


from dataclasses import dataclass
from typing import List, Self, Tuple

from ..connection import RedisConnection
from ..data_structs import EntryId, RedisStream
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class XaddCommand(RedisCommand):
    key: bytes
    id_str: str
    fvpairs: List[Tuple[bytes, bytes]]

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            stream = database.setdefault(self.key, RedisStream())
            if not isinstance(stream, RedisStream):
                raise RuntimeError('WRONGTYPE')

            entry_id = self._parse_id()
            stream.add(entry_id, self.fvpairs)

            return RespBulkString(str(entry_id))

    def _parse_id(self) -> EntryId:
        ms_time, seq_num = (int(s) for s in self.id_str.split('-'))
        return EntryId(ms_time, seq_num)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) < 4 or len(args) % 2 != 0:
            raise RuntimeError('XADD command syntax: XADD key id field value [field value ...]')
        return cls(
            key=args[0],
            id_str=args[1].decode(),
            fvpairs=list(zip(args[2::2], args[3::2])),
        )
