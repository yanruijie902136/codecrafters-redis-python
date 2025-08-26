from datetime import datetime, timedelta
from typing import Optional, Self


class Expiry(datetime):
    @classmethod
    def from_kwargs(cls, *, px: Optional[int] = None) -> Optional[Self]:
        if px is not None:
            return cls.now() + timedelta(milliseconds=px)
        return None

    def has_passed(self) -> bool:
        return self <= self.now()
