import threading
from pathlib import Path

import numpy as np
import pytest

from ai import protocol
from ai.media import Tools
from ai.runtime import Runtime
from ai.tasks import Context, nsfw


class FakeSession:
    def __init__(self, scores, width=2):
        self.scores, self.i, self.width = list(scores), 0, width

    def get_inputs(self):
        return [type("I", (), {"name": "input:0"})]

    def get_outputs(self):
        return [type("O", (), {"name": "outputs"})]

    def run(self, _names, feeds):
        n = feeds["input:0"].shape[0]
        s = self.scores[self.i : self.i + n]
        self.i += n
        if self.width == 1:
            return [np.array(s, dtype=np.float32).reshape(-1, 1)]
        return [np.array([[1 - v, v] for v in s], dtype=np.float32)]


def ctx():
    return Context(
        runtime=Runtime(device="cpu"), tools=Tools(), model_dir=Path("."), cancelled=threading.Event()
    )


def frames(n):
    return (np.zeros((256, 256, 3), dtype=np.uint8) for _ in range(n))


@pytest.fixture(autouse=True)
def quiet(monkeypatch):
    sent = []
    monkeypatch.setattr(protocol, "send", sent.append)
    return sent


def scan(scores, n, batch=4, fps=None, width=2, c=None):
    det = nsfw.Detector(FakeSession(scores, width))
    return nsfw._scan("j", frames(n), det, c or ctx(), 0.8, batch, fps, n)


def test_safe_takes_the_max_and_scores_the_last_partial_batch():
    out = scan([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], 6)
    assert out["status"] == "safe"
    assert out["frameCount"] == 6
    assert out["maxScore"] == pytest.approx(0.6)


def test_stops_at_the_first_hit_with_its_index_and_timestamp():
    out = scan([0.1, 0.1, 0.1, 0.1, 0.1, 0.95, 0.1, 0.1, 0.99], 9, fps=2.0)
    assert out["status"] == "nsfw"
    assert out["detectedFrameIndex"] == 6
    assert out["detectedTimestamp"] == pytest.approx(2.5)
    assert out["frameCount"] == 8  # stopped after the batch with the hit


def test_single_column_output_is_accepted():
    assert scan([0.9], 1, width=1)["status"] == "nsfw"


def test_wrong_output_shape_is_an_error_not_a_guess():
    class Bad(FakeSession):
        def run(self, _n, feeds):
            return [np.zeros((feeds["input:0"].shape[0], 3), dtype=np.float32)]

    with pytest.raises(protocol.TaskError):
        nsfw._scan("j", frames(1), nsfw.Detector(Bad([])), ctx(), 0.8, 4, None, 1)


def test_cancel_stops_the_scan():
    c = ctx()
    c.cancelled.set()
    with pytest.raises(protocol.Cancelled):
        scan([0.1] * 8, 8, c=c)


def test_no_frames_is_a_media_error():
    with pytest.raises(protocol.TaskError) as e:
        scan([], 0)
    assert e.value.code == "media"


def test_progress_is_reported(quiet):
    scan([0.1] * 8, 8)
    progress = [m for m in quiet if m["type"] == "progress"]
    assert progress[-1]["processed"] == 8 and progress[-1]["fraction"] == 1.0


@pytest.mark.parametrize(
    "src_fps,total,want,expect",
    [
        (30.0, 300, 2.0, (2.0, 2.0, 20)),  # sample a 10 s clip at 2 fps
        (1.0, 10, 2.0, (0.0, 1.0, 10)),  # source slower than the rate: every frame
        (None, None, 2.0, (2.0, 2.0, None)),  # unknown fps: sample
        (30.0, 300, 0.0, (0.0, 30.0, 300)),  # 0 = every frame
    ],
)
def test_plan_sampling(src_fps, total, want, expect):
    assert nsfw.plan_sampling(src_fps, total, want) == expect


def test_preprocess_matches_the_model_contract():
    out = nsfw.preprocess(np.full((480, 640, 3), 128, dtype=np.uint8))
    assert out.shape == (224, 224, 3) and out.dtype == np.float32
    # Grey 128 in BGR minus the per-channel mean.
    assert out[100, 100] == pytest.approx([24, 11, 5], abs=2)
