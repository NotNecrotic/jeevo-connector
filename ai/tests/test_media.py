import shutil
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from ai import media

HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def assert_guarded(cmd, path):
    assert cmd[cmd.index("-protocol_whitelist") + 1] == "file"
    formats = cmd[cmd.index("-format_whitelist") + 1].split(",")
    assert {"mov", "matroska", "avi"} <= set(formats)
    assert not {"hls", "applehttp", "concat", "sdp", "rtsp"} & set(formats)
    # Input options apply to the input that follows them.
    assert cmd.index("-protocol_whitelist") < cmd.index(path)
    assert cmd.index("-format_whitelist") < cmd.index(path)


def test_probe_command_is_guarded(monkeypatch):
    seen = []

    def fake_run(cmd, **kwargs):
        seen.append(cmd)
        return SimpleNamespace(stdout="{}")

    monkeypatch.setattr(media.subprocess, "run", fake_run)
    media.probe("/data/original.mp4", media.Tools())
    assert_guarded(seen[0], "/data/original.mp4")


def test_frames_command_is_guarded(monkeypatch):
    seen = []

    def fake_popen(cmd, **kwargs):
        seen.append(cmd)
        raise OSError("no ffmpeg in this test")

    monkeypatch.setattr(media.subprocess, "Popen", fake_popen)
    with pytest.raises(media.MediaError):
        next(media.frames("/data/original.mp4", media.Tools()))
    assert_guarded(seen[0], "/data/original.mp4")


@pytest.mark.skipif(not HAVE_FFMPEG, reason="ffmpeg not installed")
def test_probe_refuses_a_playlist_named_like_a_video(tmp_path):
    real = tmp_path / "real.mp4"
    subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10", "-t", "1", str(real)],
        check=True,
    )
    fake = tmp_path / "upload.mp4"
    fake.write_text("ffconcat version 1.0\nfile real.mp4\n")
    assert media.probe(str(real), media.Tools()).fps == 10
    # Without the guard ffprobe would follow the concat file to real.mp4.
    assert media.probe(str(fake), media.Tools()).fps is None


def test_prefetch_keeps_order():
    assert list(media.prefetch(range(100), 3)) == list(range(100))


def test_prefetch_reraises_source_error_after_items():
    def source():
        yield 1
        yield 2
        raise media.MediaError("decode failed")

    got = []
    with pytest.raises(media.MediaError, match="decode failed"):
        for item in media.prefetch(source(), 2):
            got.append(item)
    assert got == [1, 2]


def test_prefetch_early_stop_closes_source():
    closed = threading.Event()

    def source():
        try:
            yield from range(1_000_000)
        finally:
            closed.set()

    it = media.prefetch(source(), 2)
    assert next(it) == 0
    it.close()
    assert closed.is_set()


def test_prefetch_runs_ahead_but_stays_bounded():
    pulled = []

    def source():
        for i in range(50):
            pulled.append(i)
            yield i

    it = media.prefetch(source(), 4)
    assert next(it) == 0
    deadline = time.monotonic() + 5
    while len(pulled) < 5 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.2)
    # 1 consumed + 4 queued + 1 held by the blocked producer
    assert 5 <= len(pulled) <= 6
    it.close()
