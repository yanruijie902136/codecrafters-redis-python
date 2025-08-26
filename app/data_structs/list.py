from typing import List


class RedisList:
    def __init__(self) -> None:
        self._elements: List[bytes] = []

    def get_range(self, start: int, stop: int) -> List[bytes]:
        if stop == -1:
            return self._elements[start:]
        return self._elements[start:stop+1]

    def rpush(self, elements: List[bytes]) -> None:
        self._elements.extend(elements)

    def __len__(self) -> int:
        return len(self._elements)
