#!/usr/bin/env python3

"""
Per-caller round-robin FIFO scheduler.

Every request carries a caller id (script/pid@host). Each caller gets its own
FIFO sub-queue; the scheduler rotates between callers so a bulk job (e.g.
``document_augmented_stora`` issuing thousands of calls) cannot starve a small
cron script. Within a caller, order is strict FIFO.

No framework or network dependency: pure threading primitives, fully unit
testable.

2026
"""

import threading
import time
from collections import OrderedDict, deque
from typing import Any, Dict, Optional


class RoundRobinQueue:
    """Thread-safe, per-caller round-robin queue with a bounded depth."""

    def __init__(self, max_depth: int = 500) -> None:
        self._cond = threading.Condition()
        self._callers: "OrderedDict[str, deque]" = OrderedDict()
        self._order: deque = deque()
        self._size = 0
        self._max_depth = max_depth
        self._closed = False

    def submit(self, caller: str, item: Any) -> bool:
        """
        Enqueue ``item`` for ``caller``. Returns ``False`` when the queue is at
        capacity (caller should retry later / receive 503).
        """
        with self._cond:
            if self._closed:
                return False
            if self._size >= self._max_depth:
                return False
            dq = self._callers.get(caller)
            if dq is None:
                dq = deque()
                self._callers[caller] = dq
                self._order.append(caller)
            dq.append(item)
            self._size += 1
            self._cond.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """
        Remove and return the next item using round-robin across callers.
        Blocks up to ``timeout`` seconds; returns ``None`` on timeout or when
        the queue is closed and drained.
        """
        with self._cond:
            deadline = None if timeout is None else time.monotonic() + timeout
            while self._size == 0:
                if self._closed:
                    return None
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                self._cond.wait(remaining)

            caller = self._order[0]
            dq = self._callers[caller]
            item = dq.popleft()
            self._size -= 1
            if dq:
                self._order.rotate(-1)
            else:
                del self._callers[caller]
                self._order.popleft()
            return item

    def depth(self) -> int:
        with self._cond:
            return self._size

    def depths_by_caller(self) -> Dict[str, int]:
        with self._cond:
            return {caller: len(dq) for caller, dq in self._callers.items()}

    def close(self) -> None:
        """Close the queue and wake any waiting consumers."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
