"""Compatibility shim. Use ``python -m taskgrid.worker`` for new code."""
from taskgrid.worker import *  # noqa: F401,F403
from taskgrid.worker import main

if __name__ == "__main__":
    raise SystemExit(main())
