from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from ..media import Tools
from ..runtime import Runtime


@dataclass
class Context:
    runtime: Runtime
    tools: Tools
    model_dir: Path
    cancelled: threading.Event = field(default_factory=threading.Event)


Task = Callable[[str, dict, Context], dict]


def registry() -> dict[str, Task]:
    from . import nsfw

    return {"nsfw_scan": nsfw.run}


def warm_up(ctx: Context) -> dict[str, str]:
    from . import nsfw

    return nsfw.warm_up(ctx)
