from __future__ import annotations

import json
import queue
import subprocess
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TypeVar

import numpy as np

SIZE = 256
FRAME_BYTES = SIZE * SIZE * 3

INPUT_GUARD = [
    "-protocol_whitelist",
    "file",
    "-format_whitelist",
    "mov,matroska,avi,image2,jpeg_pipe,png_pipe,webp_pipe,gif",
]


T = TypeVar("T")


class MediaError(RuntimeError):
    """The file could not be decoded. Not retryable: the file will not change."""


@dataclass
class Tools:
    ffmpeg: str = "ffmpeg"
    ffprobe: str = "ffprobe"


@dataclass
class VideoInfo:
    fps: float | None
    total_frames: int | None
    duration: float | None


def _rate(raw) -> float | None:
    try:
        text = str(raw)
        if "/" in text:
            n, d = (float(x) for x in text.split("/", 1))
            return n / d if n > 0 and d > 0 else None
        v = float(text)
        return v if v > 0 else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def probe(path: str, tools: Tools) -> VideoInfo:
    """Frame rate and frame count from container metadata (never a full decode)."""
    try:
        out = subprocess.run(
            [
                tools.ffprobe,
                "-v",
                "error",
                *INPUT_GUARD,
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=avg_frame_rate,r_frame_rate,nb_frames:format=duration",
                "-of",
                "json",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        data = json.loads(out.stdout or "{}")
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise MediaError(f"ffprobe failed: {exc}") from exc
    stream = (data.get("streams") or [{}])[0]
    fps = _rate(stream.get("avg_frame_rate")) or _rate(stream.get("r_frame_rate"))
    duration = _rate((data.get("format") or {}).get("duration"))
    try:
        total = int(stream.get("nb_frames") or 0) or None
    except ValueError:
        total = None
    if total is None and fps and duration:
        total = round(fps * duration)
    return VideoInfo(fps=fps, total_frames=total, duration=duration)


def frames(path: str, tools: Tools, sample_fps: float = 0.0, limit: int = 0) -> Iterator[np.ndarray]:
    """Yield uint8 256x256x3 RGB frames; `sample_fps` samples inside ffmpeg, `limit` caps the count."""
    vf = f"scale={SIZE}:{SIZE}:flags=bilinear"
    if sample_fps > 0:
        vf = f"fps={sample_fps:g}," + vf
    cmd = [
        tools.ffmpeg,
        "-nostdin",
        "-v",
        "error",
        *INPUT_GUARD,
        "-i",
        path,
        "-an",
        "-sn",
        "-vf",
        vf,
        "-fps_mode",
        "passthrough",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-",
    ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=FRAME_BYTES * 2)
    except OSError as exc:
        raise MediaError(f"could not start ffmpeg: {exc}") from exc
    errors: list = []
    drain = threading.Thread(target=lambda: errors.append(proc.stderr.read()), daemon=True)
    drain.start()
    count = 0
    finished = False
    try:
        while not limit or count < limit:
            buf = proc.stdout.read(FRAME_BYTES)
            if len(buf) < FRAME_BYTES:
                finished = True
                break
            count += 1
            yield np.frombuffer(buf, dtype=np.uint8).reshape(SIZE, SIZE, 3)
    finally:
        proc.stdout.close()
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        drain.join(timeout=5)
    stderr = (errors[0] if errors else b"").decode("utf-8", "replace").strip()
    if finished and proc.returncode != 0:
        raise MediaError(f"ffmpeg exited with {proc.returncode}: {stderr[-400:]}")
    if finished and count == 0:
        raise MediaError(f"no frames could be decoded{': ' + stderr[-200:] if stderr else ''}")


def prefetch(source: Iterable[T], depth: int) -> Iterator[T]:
    """Prefetch `source` in a thread; closing it on early exit is what stops ffmpeg in `frames`."""
    q: queue.Queue = queue.Queue(maxsize=max(1, depth))
    stop = threading.Event()
    done = object()

    def put(item) -> bool:
        while not stop.is_set():
            try:
                q.put(item, timeout=0.1)
                return True
            except queue.Full:
                pass
        return False

    def produce() -> None:
        it = iter(source)
        try:
            for item in it:
                if not put((None, item)):
                    return
            put((None, done))
        except BaseException as exc:
            put((exc, None))
        finally:
            close = getattr(it, "close", None)
            if close:
                close()

    worker = threading.Thread(target=produce, daemon=True)
    worker.start()
    try:
        while True:
            exc, item = q.get()
            if exc is not None:
                raise exc
            if item is done:
                return
            yield item
    finally:
        stop.set()
        worker.join()
