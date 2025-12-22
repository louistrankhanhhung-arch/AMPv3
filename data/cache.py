from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class CacheItem:
    value: Any
    expires_at: float


class TTLCache:
    """
    Simple in-memory TTL cache.
    On Railway, process can restart; this is fine for Tầng 1.
    Tầng sau bạn có thể chuyển sang Redis/SQLite nếu cần.
    """
    def __init__(self) -> None:
        self._store: Dict[Tuple[str, ...], CacheItem] = {}

    def get(self, key: Tuple[str, ...]) -> Optional[Any]:
        item = self._store.get(key)
        if not item:
            return None
        if time.time() >= item.expires_at:
            self._store.pop(key, None)
            return None
        return item.value

    def set(self, key: Tuple[str, ...], value: Any, ttl_sec: int) -> None:
        self._store[key] = CacheItem(value=value, expires_at=time.time() + ttl_sec)

    def clear(self) -> None:
        self._store.clear()
