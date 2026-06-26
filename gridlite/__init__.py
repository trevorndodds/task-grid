"""Compatibility shim for the old ``gridlite`` package name.

Use ``taskgrid`` for new code.
"""
try:
    from taskgrid import *  # noqa: F401,F403
except Exception:
    pass
