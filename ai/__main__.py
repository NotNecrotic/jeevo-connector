from __future__ import annotations

import argparse
import json
import logging
import queue
import sys
import threading
from pathlib import Path

from . import protocol, runtime
from .media import Tools
from .models import ModelError
from .tasks import Context, registry, warm_up


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m ai")
    ap.add_argument("--model-dir", required=True, type=Path)
    ap.add_argument("--device", default="auto", choices=["auto", "cpu", "cuda"])
    ap.add_argument("--gpu-id", type=int, default=0)
    ap.add_argument("--cpu-threads", type=int, default=0)
    ap.add_argument("--ffmpeg", default="ffmpeg")
    ap.add_argument("--ffprobe", default="ffprobe")
    ap.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    args = ap.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=args.log_level.upper(), format="%(levelname)s %(message)s")
    log = logging.getLogger("ai")

    try:
        rt = runtime.create(args.device, args.gpu_id, args.cpu_threads)
        ctx = Context(runtime=rt, tools=Tools(args.ffmpeg, args.ffprobe), model_dir=args.model_dir)
        models = warm_up(ctx)
    except (runtime.RuntimeSetupError, ModelError) as exc:
        protocol.fatal(str(exc))
        log.error("%s", exc)
        return 2
    tasks = registry()
    protocol.ready(
        device=rt.active,
        gpu=rt.gpu_name,
        providers=rt.providers,
        tasks=sorted(tasks),
        models=models,
    )
    log.info("ready on %s%s", rt.active, f" ({rt.gpu_name})" if rt.gpu_name else "")

    jobs: queue.Queue[dict | None] = queue.Queue()
    current = {"id": None}

    def reader() -> None:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                log.warning("ignoring a line that is not JSON")
                continue
            kind = msg.get("type")
            if kind == "job":
                jobs.put(msg)
            elif kind == "cancel":
                if msg.get("id") and msg.get("id") == current["id"]:
                    ctx.cancelled.set()
            elif kind == "shutdown":
                break
        jobs.put(None)

    threading.Thread(target=reader, daemon=True).start()

    while True:
        msg = jobs.get()
        if msg is None:
            return 0
        job_id = str(msg.get("id") or "")
        task = tasks.get(str(msg.get("task")))
        if not job_id or task is None:
            protocol.error(job_id, "bad_input", f"unknown task {msg.get('task')!r}", retryable=False)
            continue
        current["id"] = job_id
        ctx.cancelled.clear()
        try:
            protocol.result(job_id, task(job_id, msg.get("input") or {}, ctx))
        except protocol.TaskError as exc:
            protocol.error(job_id, exc.code, str(exc), exc.retryable)
        except Exception as exc:
            log.exception("job %s crashed", job_id)
            protocol.error(job_id, "internal", f"{type(exc).__name__}: {exc}", retryable=True)
        finally:
            current["id"] = None


if __name__ == "__main__":
    sys.exit(main())
