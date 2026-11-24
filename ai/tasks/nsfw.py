from __future__ import annotations

import io
import logging
import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from .. import protocol
from ..media import SIZE, MediaError, frames, prefetch, probe
from ..models import MODELS, ensure
from . import Context

log = logging.getLogger("ai")

SPEC = MODELS["opennsfw2"]
CROP = 224
BGR_MEAN = np.array([104.0, 117.0, 123.0], dtype=np.float32)
IMAGE_TYPES = {"photo", "gif"}
PREFETCH_BATCHES = 2
MAX_IMAGE_FRAMES = 3000


def preprocess(rgb: np.ndarray) -> np.ndarray:
    """uint8 HxWx3 RGB -> float32 224x224x3 BGR, mean-subtracted."""
    from PIL import Image

    img = Image.fromarray(rgb, mode="RGB").resize((SIZE, SIZE), Image.BILINEAR)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    buf.seek(0)
    img = Image.open(buf).convert("RGB")
    off = (SIZE - CROP) // 2
    arr = np.asarray(img.crop((off, off, off + CROP, off + CROP)), dtype=np.float32)
    return np.ascontiguousarray(arr[:, :, ::-1] - BGR_MEAN, dtype=np.float32)


class Detector:
    def __init__(self, session):
        self.session = session
        self.input = session.get_inputs()[0].name
        self.output = session.get_outputs()[0].name
        self.pool = ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 1))

    def score(self, batch: list[np.ndarray]) -> list[float]:
        tensor = np.stack(list(self.pool.map(preprocess, batch)), axis=0)
        try:
            out = np.asarray(self.session.run([self.output], {self.input: tensor})[0], dtype=np.float32)
        except Exception as exc:
            raise protocol.TaskError("inference", f"inference failed: {exc}", retryable=True) from exc
        if out.ndim == 1:
            out = out.reshape(1, -1)
        if out.ndim != 2 or out.shape[1] not in (1, 2) or out.shape[0] != len(batch):
            raise protocol.TaskError("inference", f"unexpected model output shape {out.shape}")
        return [float(s) for s in out[:, -1]]


_detector: Detector | None = None


def warm_up(ctx: Context) -> dict:
    global _detector
    if _detector is None:
        path = ensure(SPEC, ctx.model_dir)
        _detector = Detector(ctx.runtime.session(str(path)))
    return {SPEC.name: SPEC.version}


def plan_sampling(src_fps: float | None, src_total: int | None, want: float):
    if want > 0 and (not src_fps or want < src_fps):
        total = max(1, round(src_total * want / src_fps)) if src_total and src_fps else None
        return want, want, total
    return 0.0, src_fps, src_total


def _scan(
    job_id: str,
    stream: Iterable[np.ndarray],
    det: Detector,
    ctx: Context,
    threshold: float,
    batch_size: int,
    fps: float | None,
    total: int | None,
) -> dict:
    max_score, processed, hit = 0.0, 0, None
    batch: list[np.ndarray] = []

    def flush() -> int | None:
        nonlocal max_score, processed
        if ctx.cancelled.is_set():
            raise protocol.Cancelled()
        scores = det.score(batch)
        max_score = max(max_score, max(scores))
        first = next((i for i, s in enumerate(scores) if s >= threshold), None)
        start = processed
        processed += len(batch)
        batch.clear()
        protocol.progress(job_id, processed, total)
        return start + first + 1 if first is not None else None

    ahead = prefetch(stream, batch_size * PREFETCH_BATCHES)
    try:
        for frame in ahead:
            batch.append(frame)
            if len(batch) >= batch_size:
                hit = flush()
                if hit is not None:
                    break
    finally:
        ahead.close()
    if hit is None and batch:
        hit = flush()
    if processed == 0:
        raise protocol.TaskError("media", "no frames could be decoded")
    return {
        "status": "nsfw" if hit is not None else "safe",
        "maxScore": round(max_score, 6),
        "frameCount": processed,
        "detectedFrameIndex": hit,
        "detectedTimestamp": round((hit - 1) / fps, 3) if hit is not None and fps else None,
        "model": SPEC.name,
        "modelVersion": SPEC.version,
        "threshold": threshold,
    }


def _still(path: str) -> np.ndarray | None:
    from PIL import Image, ImageOps

    try:
        with Image.open(path) as im:
            if getattr(im, "n_frames", 1) > 1:
                return None
            return np.asarray(ImageOps.exif_transpose(im).convert("RGB"))
    except Exception:
        return None


def run(job_id: str, inp: dict, ctx: Context) -> dict:
    path = inp.get("path")
    media_type = str(inp.get("mediaType") or "").lower()
    threshold = float(inp.get("threshold", 0.8))
    batch_size = int(inp.get("batchSize", 64))
    want_fps = float(inp.get("sampleFps", 2.0))
    if not path or not os.path.isfile(path):
        raise protocol.TaskError("bad_input", f"media file not found: {path}")
    if not 0 < threshold < 1 or batch_size < 1 or want_fps < 0:
        raise protocol.TaskError("bad_input", "threshold, batchSize or sampleFps out of range")
    warm_up(ctx)
    det = _detector
    assert det is not None
    try:
        if media_type in IMAGE_TYPES:
            still = _still(path)
            stream = [still] if still is not None else frames(path, ctx.tools, limit=MAX_IMAGE_FRAMES)
            return _scan(job_id, stream, det, ctx, threshold, batch_size, None, None)
        info = probe(path, ctx.tools)
        sample_fps, fps, total = plan_sampling(info.fps, info.total_frames, want_fps)
        stream = frames(path, ctx.tools, sample_fps=sample_fps)
        return _scan(job_id, stream, det, ctx, threshold, batch_size, fps, total)
    except MediaError as exc:
        raise protocol.TaskError("media", str(exc), retryable=False) from exc
