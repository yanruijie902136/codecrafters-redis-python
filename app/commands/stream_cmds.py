__all__ = ('XaddCommand', 'XrangeCommand', 'XreadCommand')


from dataclasses import dataclass
from typing import List, Self, Tuple

from ..connection import RedisConnection
from ..data_structs import EntryId, RedisStream, StreamEntry
from ..protocol import *

from .base import RedisCommand


def _entry_to_array(entry: StreamEntry) -> RespArray:
    return RespArray([
        RespBulkString(str(entry.id)),
        RespArray([
            RespBulkString(b) for pair in entry.fv.items() for b in pair
        ]),
    ])


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

            try:
                entry_id = self._parse_id(stream)
            except ValueError:
                return RespSimpleError('ERR The ID specified in XADD must be greater than 0-0')

            try:
                stream.add(entry_id, self.fvpairs)
            except ValueError:
                return RespSimpleError('ERR The ID specified in XADD is equal or smaller than the target stream top item')

            return RespBulkString(str(entry_id))

    def _parse_id(self, stream: RedisStream) -> EntryId:
        if self.id_str == '*':
            return stream.auto_gen_next_id()

        ms_time_str, seq_num_str = self.id_str.split('-')

        ms_time = int(ms_time_str)
        if seq_num_str == '*':
            return stream.auto_gen_next_id(ms_time)

        seq_num = int(seq_num_str)
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


@dataclass(frozen=True)
class XrangeCommand(RedisCommand):
    key: bytes
    start_id_str: str
    end_id_str: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            stream = database.get(self.key)
            if stream is None:
                return RespArray([])

            if not isinstance(stream, RedisStream):
                raise RuntimeError('WRONGTYPE')

            entries = stream.get_range(self._parse_start_id(), self._parse_end_id(stream))
            return RespArray([_entry_to_array(e) for e in entries])

    def _parse_start_id(self) -> EntryId:
        if self.start_id_str == '-':
            return EntryId(0, 1)

        if '-' not in self.start_id_str:
            ms_time = int(self.start_id_str)
            return EntryId(ms_time, 0 if ms_time > 0 else 1)

        ms_time, seq_num = (int(s) for s in self.start_id_str.split('-'))
        return EntryId(ms_time, seq_num)

    def _parse_end_id(self, stream: RedisStream) -> EntryId:
        if self.end_id_str == '+':
            return stream.auto_gen_next_id()

        if '-' not in self.end_id_str:
            ms_time = int(self.end_id_str)
            return stream.auto_gen_next_id(ms_time)

        ms_time, seq_num = (int(s) for s in self.end_id_str.split('-'))
        return EntryId(ms_time, seq_num + 1)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 3:
            raise RuntimeError('XRANGE command syntax: XRANGE key start end')

        return cls(
            key=args[0],
            start_id_str=args[1].decode(),
            end_id_str=args[2].decode(),
        )


@dataclass(frozen=True)
class XreadCommand(RedisCommand):
    key: bytes
    id_str: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            stream = database.get(self.key)
            if stream is None:
                return RespNullBulkString

            if not isinstance(stream, RedisStream):
                raise RuntimeError('WRONGTYPE')

            entries = stream.read(self._parse_id())
            return RespArray([
                RespArray([
                    RespBulkString(self.key),
                    RespArray([
                        _entry_to_array(e) for e in entries
                    ]),
                ]),
            ])

    def _parse_id(self) -> EntryId:
        ms_time, seq_num = (int(s) for s in self.id_str.split('-'))
        return EntryId(ms_time, seq_num + 1)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 3 or args[0].upper() != b'STREAMS':
            raise RuntimeError('XREAD command syntax: XREAD STREAMS key id')

        return cls(key=args[1], id_str=args[2].decode())
