from __future__ import annotations

import collections
import dataclasses


auto_seq_numbers = collections.defaultdict(int)


@dataclasses.dataclass(order=True)
class StreamEntryId:
    msec: int
    seq_number: int

    @classmethod
    def from_string(cls, string: str) -> StreamEntryId:
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
class StreamEntry:
    entry_id: StreamEntryId
    data: dict[str, str]


class RedisStream:
    def __init__(self):
        self._entries: list[StreamEntry] = []

    def add_entry(self, entry_id: StreamEntryId, data: dict[str, str]) -> bool:
        if self._entries and entry_id <= self._entries[-1].entry_id:
            return False
        self._entries.append(StreamEntry(entry_id, data))
        return True
