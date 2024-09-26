from __future__ import annotations

import dataclasses


@dataclasses.dataclass(order=True)
class StreamEntryId:
    msec: int
    seq_number: int

    @classmethod
    def from_string(cls, string: str) -> StreamEntryId:
        msec, seq_number = map(int, string.split("-"))
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
