__all__ = ('EntryId', 'RedisStream', 'StreamEntry')


from bisect import bisect_left
from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Optional, Tuple


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
        self._max_seq_nums: dict[int, int] = {}

    def add(self, entry_id: EntryId, fvpairs: List[Tuple[bytes, bytes]]) -> None:
        if self._entries and entry_id <= self._entries[-1].id:
            raise ValueError

        self._max_seq_nums[entry_id.ms_time] = entry_id.seq_num
        self._entries.append(StreamEntry(entry_id, OrderedDict(fvpairs)))

    def auto_gen_next_id(self, ms_time: Optional[int] = None) -> EntryId:
        if ms_time is None:
            if not self._entries:
                return EntryId(0, 1)
            last_id = self._entries[-1].id
            return EntryId(last_id.ms_time, last_id.seq_num + 1)

        max_seq_num = self._max_seq_nums.get(ms_time, None)
        if max_seq_num is None:
            seq_num = 0 if ms_time > 0 else 1
        else:
            seq_num = max_seq_num + 1
        return EntryId(ms_time, seq_num)

    def get_range(self, start_id: EntryId, end_id: EntryId) -> List[StreamEntry]:
        return [e for e in self._entries if start_id <= e.id < end_id]

    def read(self, start_id: EntryId) -> List[StreamEntry]:
        i = bisect_left(self._entries, start_id, key=lambda e: e.id)
        return self._entries[i:]
