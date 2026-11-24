import hashlib
import io

import pytest

from ai import models


def spec_for(data: bytes) -> models.ModelSpec:
    return models.ModelSpec(
        "m", "v1", "m.onnx", "https://example.invalid/m", hashlib.sha256(data).hexdigest(), len(data)
    )


class Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_download_verifies_and_caches(tmp_path, monkeypatch):
    data = b"model-bytes" * 100
    calls = []
    monkeypatch.setattr(models.urllib.request, "urlopen", lambda *a, **k: calls.append(1) or Resp(data))
    p = models.ensure(spec_for(data), tmp_path)
    assert p.read_bytes() == data
    models.ensure(spec_for(data), tmp_path)
    assert len(calls) == 1, "a verified cache hit does not download again"


def test_checksum_mismatch_is_never_kept(tmp_path, monkeypatch):
    monkeypatch.setattr(models.urllib.request, "urlopen", lambda *a, **k: Resp(b"tampered"))
    with pytest.raises(models.ModelError):
        models.ensure(spec_for(b"original"), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_corrupt_cache_is_replaced(tmp_path, monkeypatch):
    data = b"good"
    (tmp_path / "m.onnx").write_bytes(b"bad")
    monkeypatch.setattr(models.urllib.request, "urlopen", lambda *a, **k: Resp(data))
    assert models.ensure(spec_for(data), tmp_path).read_bytes() == data
