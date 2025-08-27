__all__ = ('EntryId', 'RedisStream', 'StreamEntry')


from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True, order=True)
class EntryId:
    ms_time: int
    seq_num: int

    def __post_init__(self) -> None:
        if (self.ms_time, self.seq_num) <= (0, 0):
            raise ValueError

    def __str__(self) -> str:
        return f'{self.ms_time}-{self.seq_num}'


@dataclass
class StreamEntry:
    id: EntryId
    fv: OrderedDict[bytes, bytes]


class RedisStream:
    def __init__(self) -> None:
        self._entries: List[StreamEntry] = []

    def add(self, entry_id: EntryId, fvpairs: List[Tuple[bytes, bytes]]) -> None:
        if self._entries and entry_id <= self._entries[-1].id:
            raise ValueError
        self._entries.append(StreamEntry(entry_id, OrderedDict(fvpairs)))
