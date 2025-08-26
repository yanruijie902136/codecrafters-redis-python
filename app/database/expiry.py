from datetime import datetime, timedelta
from typing import Optional, Self


class Expiry(datetime):
    @classmethod
    def from_kwargs(cls, *, px: Optional[int] = None, exat: Optional[int] = None, pxat: Optional[int] = None) -> Optional[Self]:
        if px is not None:
            return cls.now() + timedelta(milliseconds=px)
        elif exat is not None:
            return cls.fromtimestamp(exat)
        elif pxat is not None:
            return cls.fromtimestamp(pxat / 1000)
        return None

    def has_passed(self) -> bool:
        return self <= self.now()
