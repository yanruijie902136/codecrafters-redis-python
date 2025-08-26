from typing import List


class RedisList:
    def __init__(self) -> None:
        self._elements: List[bytes] = []

    def rpush(self, elements: List[bytes]) -> None:
        self._elements.extend(elements)

    def __len__(self) -> int:
        return len(self._elements)
