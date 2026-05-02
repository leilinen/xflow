from __future__ import annotations

import time
from collections.abc import Callable


def run_forever(interval_seconds: int, task: Callable[[], None]) -> None:
    while True:
        task()
        time.sleep(interval_seconds)
