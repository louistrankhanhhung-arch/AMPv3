from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Hashable
from collections import deque


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
        # Non-TTL store for rolling series / in-process state
        # NOTE: rolling series keys may be tuples or strings; accept any hashable.
        self._persist: Dict[Hashable, Any] = {}

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

    def get_persist(self, key: Tuple[str, ...]) -> Optional[Any]:
        return self._persist.get(key)

    def set_persist(self, key: Tuple[str, ...], value: Any) -> None:
        self._persist[key] = value

    def get_or_create_deque(self, key: Hashable, maxlen: int):
        """
        Convenience helper to store a rolling deque (no TTL).
        """
        d = self._persist.get(key)
        if d is None:
            d = deque(maxlen=maxlen)
            self._persist[key] = d
        return d

    def clear(self) -> None:
        self._store.clear()
        self._persist.clear()
