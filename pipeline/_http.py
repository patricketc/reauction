"""Shared HTTP helpers: session with a friendly UA, a rate limiter, and a
per-AIN JSON cache on disk so re-runs don't re-hit upstream services.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)

USER_AGENT = (
    "reauction/0.1 (personal research tool; "
    "https://github.com/patricketc/reauction)"
)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json, text/html"})
    return s


class RateLimiter:
    """Simple global rate limiter: sleep if the last call was less than
    ``min_interval`` seconds ago.
    """

    def __init__(self, min_interval: float = 1.0) -> None:
        self.min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.monotonic()


class JsonCache:
    """Writes one JSON blob per key under ``root``.

    Callers are expected to produce something JSON-serializable. Missing entries
    return ``None``.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
        return self.root / f"{safe}.json"

    def get(self, key: str) -> Any | None:
        p = self._path(key)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except (OSError, json.JSONDecodeError) as e:
            log.warning("cache read failed for %s: %s", key, e)
            return None

    def set(self, key: str, value: Any) -> None:
        p = self._path(key)
        try:
            p.write_text(json.dumps(value, indent=2, default=str))
        except OSError as e:
            log.warning("cache write failed for %s: %s", key, e)
