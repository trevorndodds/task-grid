from __future__ import annotations

import math
import time
from typing import Any

from taskgrid.tasks import task


@task("square")
def square(payload: dict[str, Any]) -> dict[str, Any]:
    x = float(payload["x"])
    return {"x": x, "square": x * x}


@task("monte_carlo_pi_chunk")
def monte_carlo_pi_chunk(payload: dict[str, Any]) -> dict[str, Any]:
    # Deterministic toy workload; avoids random so examples are reproducible.
    samples = int(payload.get("samples", 10000))
    offset = int(payload.get("offset", 0))
    inside = 0
    for i in range(samples):
        n = i + offset
        x = math.sin(n * 12.9898) * 43758.5453
        y = math.sin(n * 78.233) * 24634.6345
        x = x - math.floor(x)
        y = y - math.floor(y)
        if x * x + y * y <= 1.0:
            inside += 1
    return {"inside": inside, "samples": samples}


@task("sometimes_fails")
def sometimes_fails(payload: dict[str, Any]) -> dict[str, Any]:
    fail = bool(payload.get("fail", False))
    if fail:
        raise RuntimeError("intentional demo failure")
    time.sleep(float(payload.get("seconds", 0.2)))
    return {"ok": True}
