from __future__ import annotations

import collections
import dataclasses
import itertools
import math
from typing import Optional

from .resp2 import RespArray, RespBulkString, RespSerializable
from .utils import get_current_timestamp


auto_seq_numbers = collections.defaultdict(int)


@dataclasses.dataclass(order=True)
class StreamEntryId:
    msec: int
    seq_number: int

    @classmethod
    def from_string(cls, string: str) -> StreamEntryId:
        if string == "*":
            return StreamEntryId(int(get_current_timestamp()), 0)

        msec, seq_number = string.split("-")
        msec = int(msec)
        if seq_number == "*":
            auto_seq_numbers[msec] += 1
            seq_number = auto_seq_numbers[msec]
            if msec > 0:
                seq_number -= 1
        else:
            seq_number = int(seq_number)
        return StreamEntryId(msec, seq_number)

    def __str__(self) -> str:
        return f"{self.msec}-{self.seq_number}"


@dataclasses.dataclass
class StreamEntry(RespSerializable):
    entry_id: StreamEntryId
    data: dict[str, str]

    def serialize(self) -> bytes:
        return RespArray([
            RespBulkString(str(self.entry_id)),
            RespArray([
                RespBulkString(s) for s in itertools.chain(*self.data.items())
            ]),
        ]).serialize()


class RedisStream:
    def __init__(self):
        self._entries: list[StreamEntry] = []

    def add_entry(self, entry_id: StreamEntryId, data: dict[str, str]) -> bool:
        if self._entries and entry_id <= self._entries[-1].entry_id:
            return False
        self._entries.append(StreamEntry(entry_id, data))
        return True

    def xrange(
        self,
        start: Optional[StreamEntryId] = None,
        end: Optional[StreamEntryId] = None,
    ) -> RespArray:
        if start is None:
            start = StreamEntryId(0, 0)
        if end is None:
            end = StreamEntryId(math.inf, math.inf)
        return RespArray([
            entry for entry in self._entries if start <= entry.entry_id <= end
        ])

    def xread(self, exclusive_start: StreamEntryId) -> RespArray:
        start = exclusive_start
        start.seq_number += 1
        return self.xrange(start)
