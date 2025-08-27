__all__ = ('XaddCommand', 'XrangeCommand', 'XreadCommand')


import time
from dataclasses import dataclass
from typing import List, Optional, Self, Tuple

from ..connection import RedisConnection
from ..data_structs import EntryId, RedisDataStruct, RedisStream, StreamEntry
from ..database import RedisDatabase
from ..protocol import *

from .base import RedisCommand


def _entry_to_resp_array(entry: StreamEntry) -> RespArray:
    return RespArray([
        RespBulkString(str(entry.id)),
        RespArray([
            RespBulkString(b) for pair in entry.fv.items() for b in pair
        ]),
    ])


def _key_and_entries_to_resp_array(key: bytes, entries: List[StreamEntry]) -> RespArray:
    return RespArray([
        RespBulkString(key),
        RespArray([
            _entry_to_resp_array(e) for e in entries
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

            database.notify(self.key)
            return RespBulkString(str(entry_id))

    def _parse_id(self, stream: RedisStream) -> EntryId:
        if self.id_str == '*':
            ms_time = int(time.time() * 1000)
            return stream.auto_gen_next_id(ms_time)

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
            return RespArray([_entry_to_resp_array(e) for e in entries])

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
    keys: List[bytes]
    id_strs: List[str]
    block_ms: Optional[int] = None

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            if self.block_ms is not None:
                return await self._block(database)
            else:
                return self._no_block(database)

    async def _block(self, database: RedisDatabase) -> RespValue:
        key = self.keys[0]
        stream = database.get(key)
        if stream is None:
            start_id = EntryId(0, 1)
        elif not isinstance(stream, RedisStream):
            raise RuntimeError('WRONGTYPE')
        else:
            id_str = self.id_strs[0]
            start_id = stream.auto_gen_next_id() if id_str == '$' else self._parse_id(id_str)

        def predicate(v: RedisDataStruct) -> bool:
            return isinstance(v, RedisStream) and bool(v.read(start_id))

        stream = await database.wait_for(key, predicate, timeout=self.block_ms / 1000)
        if stream is None:
            return RespNullBulkString
        return RespArray([_key_and_entries_to_resp_array(key, stream.read(start_id))])

    def _no_block(self, database: RedisDatabase) -> RespValue:
        values = []
        for key, id_str in zip(self.keys, self.id_strs):
            stream = database.get(key)
            if stream is None:
                continue

            if not isinstance(stream, RedisStream):
                raise RuntimeError('WRONGTYPE')

            entries = stream.read(self._parse_id(id_str))
            if entries:
                values.append(_key_and_entries_to_resp_array(key, entries))

        return RespArray(values) if values else RespNullBulkString

    def _parse_id(self, id_str: str) -> EntryId:
        ms_time, seq_num = (int(s) for s in id_str.split('-'))
        return EntryId(ms_time, seq_num + 1)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if args[0].upper() == b'BLOCK':
            block_ms = int(args[1])
            args = args[2:]
        else:
            block_ms = None

        if len(args) < 3 or len(args) % 2 == 0 or args[0].upper() != b'STREAMS':
            raise RuntimeError('XREAD command syntax: XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]')

        args = args[1:]
        n = len(args) // 2
        return cls(
            keys=args[:n],
            id_strs=[arg.decode() for arg in args[n:]],
            block_ms=block_ms
        )
