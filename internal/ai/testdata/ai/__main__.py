import json
import os
import sys


def send(m):
    sys.stdout.write(json.dumps(m) + "\n")
    sys.stdout.flush()


send({"type": "ready", "device": "cpu", "gpu": None, "providers": [], "tasks": ["nsfw_scan"], "models": {}})
for line in sys.stdin:
    m = json.loads(line)
    if m["type"] == "shutdown":
        break
    if m["type"] != "job":
        continue
    path = m["input"]["path"]
    send({"type": "progress", "id": m["id"], "fraction": 0.5, "processed": 1, "total": 2})
    if "crash" in path and not os.path.exists(path + ".crashed"):
        open(path + ".crashed", "w").close()
        os._exit(1)
    if "bad" in path:
        send(
            {"type": "error", "id": m["id"], "code": "media", "message": "cannot decode", "retryable": False}
        )
        continue
    if "slow" in path:
        for l in sys.stdin:
            c = json.loads(l)
            if c["type"] == "cancel":
                send(
                    {
                        "type": "error",
                        "id": m["id"],
                        "code": "cancelled",
                        "message": "job was cancelled",
                        "retryable": False,
                    }
                )
                break
        continue
    send(
        {
            "type": "result",
            "id": m["id"],
            "output": {
                "status": "safe",
                "maxScore": 0.01,
                "frameCount": 2,
                "detectedFrameIndex": None,
                "detectedTimestamp": None,
                "model": "fake",
                "modelVersion": "1",
                "threshold": m["input"]["threshold"],
            },
        }
    )
