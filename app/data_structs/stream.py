__all__ = ('EntryId', 'RedisStream', 'StreamEntry')


from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True, order=True)
class EntryId:
    ms_time: int
    seq_num: int

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
        entry = StreamEntry(entry_id, OrderedDict(fvpairs))
        self._entries.append(entry)
