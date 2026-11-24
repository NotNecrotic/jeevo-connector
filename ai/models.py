from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("ai")


@dataclass(frozen=True)
class ModelSpec:
    name: str
    version: str
    filename: str
    url: str
    sha256: str
    size: int


MODELS = {
    # Yahoo open_nsfw (ResNet-50), third-party ONNX conversion; NHWC [n,224,224,3] float32 BGR, output [n,2] softmax of [safe, nsfw].
    "opennsfw2": ModelSpec(
        name="opennsfw2",
        version="model-v1",
        filename="open-nsfw.onnx",
        url="https://github.com/gawryco/opennsfw-onnx/releases/download/model-v1/open-nsfw.onnx",
        sha256="864bb37bf8863564b87eb330ab8c785a79a773f4e7c43cb96db52ed8611305fa",
        size=23_590_724,
    ),
}


class ModelError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure(spec: ModelSpec, model_dir: Path) -> Path:
    """Path of the verified model file, downloading it when needed."""
    model_dir.mkdir(parents=True, exist_ok=True)
    target = model_dir / spec.filename
    if target.exists():
        if _sha256(target) == spec.sha256:
            return target
        log.warning("%s failed verification; downloading it again", target.name)
        target.unlink()
    log.info("downloading model %s %s (%d MB)", spec.name, spec.version, spec.size // 1_000_000)
    fd, tmp_name = tempfile.mkstemp(dir=model_dir, prefix=f".{spec.filename}.", suffix=".part")
    tmp = Path(tmp_name)
    digest = hashlib.sha256()
    written = 0
    try:
        req = urllib.request.Request(spec.url, headers={"User-Agent": "jeevo-connector-ai"})
        with urllib.request.urlopen(req, timeout=60) as resp, os.fdopen(fd, "wb") as out:
            for chunk in iter(lambda: resp.read(1 << 18), b""):
                written += len(chunk)
                if written > spec.size * 2:
                    raise ModelError("model download is larger than expected")
                digest.update(chunk)
                out.write(chunk)
        if digest.hexdigest() != spec.sha256:
            raise ModelError(f"{spec.name}: checksum mismatch after download")
        os.replace(tmp, target)
    except ModelError:
        raise
    except Exception as exc:
        raise ModelError(f"could not download {spec.name}: {exc}") from exc
    finally:
        if tmp.exists():
            tmp.unlink()
    return target
