from typing import Dict, List, Tuple


class RedisSortedSet:
    def __init__(self) -> None:
        self._mem2score: Dict[bytes, float] = {}

    def add(self, score_member_pairs: List[Tuple[float, bytes]]) -> int:
        new = 0
        for score, member in score_member_pairs:
            if member not in self._mem2score:
                new += 1
            self._mem2score[member] = score
        return new

    def get_range(self, start: int, stop: int) -> List[bytes]:
        sorted_members = self._sort_members()
        if stop == -1:
            return sorted_members[start:]
        return sorted_members[start:stop+1]

    def get_rank(self, member: bytes) -> int:
        return self._sort_members().index(member)

    def get_score(self, member: bytes) -> float:
        try:
            return self._mem2score[member]
        except KeyError:
            raise ValueError

    def _sort_members(self) -> List[bytes]:
        return [m for m, _ in sorted(self._mem2score.items(), key=lambda x: x[::-1])]

    def __len__(self) -> int:
        return len(self._mem2score)
