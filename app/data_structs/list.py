from typing import List, Optional, Union


class RedisList:
    def __init__(self) -> None:
        self._elements: List[bytes] = []

    def get_range(self, start: int, stop: int) -> List[bytes]:
        if stop == -1:
            return self._elements[start:]
        return self._elements[start:stop+1]

    def lpop(self, count: Optional[int] = None) -> Union[bytes, List[bytes]]:
        if count is None:
            return self._elements.pop(0)

        popped, self._elements = self._elements[:count], self._elements[count:]
        return popped

    def lpush(self, elements: List[bytes]) -> None:
        self._elements = elements[::-1] + self._elements

    def rpush(self, elements: List[bytes]) -> None:
        self._elements.extend(elements)

    def __len__(self) -> int:
        return len(self._elements)
