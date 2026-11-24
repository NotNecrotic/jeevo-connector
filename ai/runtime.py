from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field

log = logging.getLogger("ai")

CUDA = "CUDAExecutionProvider"
CPU = "CPUExecutionProvider"


class RuntimeSetupError(RuntimeError):
    pass


@dataclass
class Runtime:
    device: str
    gpu_id: int = 0
    cpu_threads: int = 0
    active: str = "cpu"
    gpu_name: str | None = None
    providers: list[str] = field(default_factory=list)
    _sessions: dict[str, object] = field(default_factory=dict)

    def session(self, model_path: str):
        if model_path not in self._sessions:
            self._sessions[model_path] = self._create(model_path)
        return self._sessions[model_path]

    def _create(self, model_path: str):
        import onnxruntime as ort

        options = ort.SessionOptions()
        options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        if self.cpu_threads > 0:
            options.intra_op_num_threads = self.cpu_threads
            options.inter_op_num_threads = 1
        providers: list = [CPU]
        if self.device != "cpu" and CUDA in ort.get_available_providers():
            providers = [(CUDA, {"device_id": self.gpu_id}), CPU]
        try:
            sess = ort.InferenceSession(model_path, sess_options=options, providers=providers)
        except Exception as exc:
            if self.device == "cuda":
                raise RuntimeSetupError(f"CUDA session failed: {exc}") from exc
            if providers[0] != CPU:
                log.warning("CUDA session failed (%s); using the CPU", exc)
                sess = ort.InferenceSession(model_path, sess_options=options, providers=[CPU])
            else:
                raise RuntimeSetupError(f"could not load {model_path}: {exc}") from exc
        active = sess.get_providers()
        on_gpu = CUDA in active
        if self.device == "cuda" and not on_gpu:
            raise RuntimeSetupError(f"device=cuda was requested but ONNX Runtime fell back to {active}")
        self.active = "cuda" if on_gpu else "cpu"
        self.providers = list(active)
        return sess


def _gpu_name(gpu_id: int) -> str | None:
    try:
        out = subprocess.run(
            ["nvidia-smi", f"--id={gpu_id}", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        name = (
            out.stdout.strip().splitlines()[0].strip() if out.returncode == 0 and out.stdout.strip() else None
        )
        return name or None
    except Exception:
        return None


def create(device: str, gpu_id: int = 0, cpu_threads: int = 0) -> Runtime:
    if device not in ("auto", "cpu", "cuda"):
        raise RuntimeSetupError(f"unknown device {device!r}")
    import onnxruntime as ort

    if device != "cpu":
        preload = getattr(ort, "preload_dlls", None)
        if preload is not None:
            try:
                preload()
            except Exception as exc:
                log.debug("preload_dlls: %s", exc)
        if CUDA not in ort.get_available_providers():
            if device == "cuda":
                raise RuntimeSetupError(
                    "device=cuda but this ONNX Runtime has no CUDA support: install the GPU "
                    "requirements (requirements-gpu.txt) or choose CPU"
                )
            device_note = "no CUDA support installed"
        else:
            device_note = "CUDA available"
        log.info("runtime: %s (%s)", device, device_note)
    rt = Runtime(device=device, gpu_id=gpu_id, cpu_threads=cpu_threads)
    if device != "cpu" and CUDA in ort.get_available_providers():
        rt.gpu_name = _gpu_name(gpu_id)
    return rt
