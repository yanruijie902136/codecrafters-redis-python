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

    def get_rank(self, member: bytes) -> int:
        sorted_members = [m for m, _ in sorted(self._mem2score.items(), key=lambda x: x[::-1])]
        return sorted_members.index(member)
