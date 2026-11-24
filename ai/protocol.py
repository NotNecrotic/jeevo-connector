from __future__ import annotations

import json
import sys
import threading
from typing import Any

_lock = threading.Lock()
_out = sys.stdout


def send(message: dict[str, Any]) -> None:
    line = json.dumps(message, separators=(",", ":"), allow_nan=False)
    with _lock:
        _out.write(line + "\n")
        _out.flush()


def ready(**fields: Any) -> None:
    send({"type": "ready", **fields})


def progress(job_id: str, processed: int, total: int | None) -> None:
    fraction = min(1.0, processed / total) if total else None
    send({"type": "progress", "id": job_id, "fraction": fraction, "processed": processed, "total": total})


def result(job_id: str, output: dict[str, Any]) -> None:
    send({"type": "result", "id": job_id, "output": output})


def error(job_id: str, code: str, message: str, retryable: bool) -> None:
    send({"type": "error", "id": job_id, "code": code, "message": message[:2000], "retryable": retryable})


def fatal(message: str) -> None:
    send({"type": "fatal", "message": message[:2000]})


class TaskError(Exception):
    """A job failed. `retryable` says whether trying again could help."""

    def __init__(self, code: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class Cancelled(TaskError):
    def __init__(self) -> None:
        super().__init__("cancelled", "job was cancelled", retryable=False)
