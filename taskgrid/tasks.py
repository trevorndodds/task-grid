from __future__ import annotations

import importlib
import os
import time
from collections.abc import Callable
from typing import Any

TaskFunc = Callable[[dict[str, Any]], Any]
REGISTRY: dict[str, TaskFunc] = {}


def task(name: str | None = None) -> Callable[[TaskFunc], TaskFunc]:
    def decorator(func: TaskFunc) -> TaskFunc:
        REGISTRY[name or func.__name__] = func
        return func

    return decorator


@task("echo")
def echo(payload: dict[str, Any]) -> dict[str, Any]:
    return {"echo": payload}


@task("add")
def add(payload: dict[str, Any]) -> dict[str, Any]:
    a = payload.get("a", 0)
    b = payload.get("b", 0)
    return {"a": a, "b": b, "sum": a + b}


@task("sleep")
def sleep_task(payload: dict[str, Any]) -> dict[str, Any]:
    seconds = float(payload.get("seconds", 1))
    time.sleep(seconds)
    return {"slept_seconds": seconds}


def load_modules(modules: list[str] | None = None) -> None:
    raw = modules or []
    env_text = os.environ.get("TASKGRID_TASK_MODULES") or os.environ.get("GRIDLITE_TASK_MODULES") or ""
    env_modules = [m.strip() for m in env_text.split(",") if m.strip()]
    for module_name in [*env_modules, *raw]:
        importlib.import_module(module_name)


def list_task_types() -> list[str]:
    return sorted(REGISTRY)


def run_task(task_type: str, payload: dict[str, Any]) -> Any:
    func = REGISTRY.get(task_type)
    if not func:
        available = ", ".join(sorted(REGISTRY)) or "none"
        raise KeyError(f"unknown task_type '{task_type}'. Available task types: {available}")
    return func(payload)
