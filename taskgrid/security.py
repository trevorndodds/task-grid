from __future__ import annotations

import hmac
import os
from dataclasses import dataclass
from typing import Iterable

from fastapi import Request


@dataclass(frozen=True)
class AuthConfig:
    admin_token: str | None
    client_token: str | None
    worker_token: str | None
    ui_token: str | None

    @property
    def enabled(self) -> bool:
        return bool(self.admin_token or self.client_token or self.worker_token or self.ui_token)


def _clean(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def config() -> AuthConfig:
    return AuthConfig(
        admin_token=_clean(os.environ.get("TASKGRID_ADMIN_TOKEN")),
        client_token=_clean(os.environ.get("TASKGRID_CLIENT_TOKEN")),
        worker_token=_clean(os.environ.get("TASKGRID_WORKER_TOKEN")),
        ui_token=_clean(os.environ.get("TASKGRID_UI_TOKEN")),
    )


def configured_roles() -> dict[str, str]:
    cfg = config()
    out: dict[str, str] = {}
    if cfg.admin_token:
        out["admin"] = cfg.admin_token
    if cfg.client_token:
        out["client"] = cfg.client_token
    if cfg.worker_token:
        out["worker"] = cfg.worker_token
    if cfg.ui_token:
        out["ui"] = cfg.ui_token
    return out


def _matches(supplied: str | None, expected: str | None) -> bool:
    return bool(supplied and expected and hmac.compare_digest(str(supplied), str(expected)))


def bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip() or None
    return None


def token_from_request(request: Request) -> str | None:
    return (
        request.headers.get("X-TaskGrid-Token")
        or request.headers.get("X-TaskGrid-Admin-Token")
        or bearer_token(request.headers.get("Authorization"))
        or request.query_params.get("api_token")
        or request.query_params.get("token")
    )


def ui_token_from_request(request: Request) -> str | None:
    return (
        request.cookies.get("taskgrid_ui_token")
        or request.query_params.get("ui_token")
        or token_from_request(request)
    )


def token_has_role(request: Request, roles: Iterable[str]) -> bool:
    cfg = config()
    roles_set = set(roles)
    supplied = token_from_request(request)
    if _matches(supplied, cfg.admin_token):
        return True
    if "client" in roles_set and _matches(supplied, cfg.client_token):
        return True
    if "worker" in roles_set and _matches(supplied, cfg.worker_token):
        return True
    # UI cookie is allowed to act as admin for browser-triggered admin/raw links.
    if "admin" in roles_set and _matches(ui_token_from_request(request), cfg.ui_token):
        return True
    return False


def ui_is_authenticated(request: Request) -> bool:
    cfg = config()
    supplied = ui_token_from_request(request)
    if not cfg.ui_token and not cfg.admin_token:
        return True
    return _matches(supplied, cfg.ui_token) or _matches(supplied, cfg.admin_token)


def role_for_path(path: str, method: str = "GET") -> tuple[str, ...] | None:
    """Return the role(s) required for an API route, or None for public routes.

    Authentication is opt-in: if the corresponding token is not configured, the
    route remains open for local/dev use. Admin tokens are accepted everywhere.
    """
    if path in {"/health", "/openapi.json", "/favicon.ico"} or path.startswith("/docs") or path.startswith("/redoc"):
        return None
    if path.startswith("/ui") or path == "/admin" or path == "/":
        return ("ui",)

    worker_exact = {"/workers/heartbeat", "/tasks/lease", "/tasks/lease-batch"}
    if path in worker_exact:
        return ("worker",)
    if path.startswith("/tasks/") and (path.endswith("/complete") or path.endswith("/fail")):
        return ("worker",)

    admin_prefixes = (
        "/maintenance",
        "/manager/log",
        "/events",
        "/workers/purge-offline",
        "/workers/bulk-state",
    )
    if path.startswith(admin_prefixes):
        return ("admin",)
    if path.startswith("/workers/") and (path.endswith("/config") or path.endswith("/disable") or path.endswith("/enable") or "/logs" in path):
        return ("admin",)
    if path == "/workers" and method.upper() == "GET":
        return ("client", "admin")

    # Client/API operations: submit, read sessions/jobs/results, retry/cancel tasks.
    if path.startswith(("/jobs", "/sessions", "/clients", "/task-catalog", "/tasks")):
        return ("client", "admin")

    return None


def configured_token_for_role(role: str) -> str | None:
    cfg = config()
    if role == "admin":
        return cfg.admin_token
    if role == "client":
        return cfg.client_token
    if role == "worker":
        return cfg.worker_token
    if role == "ui":
        return cfg.ui_token
    return None


def route_requires_auth(path: str, method: str = "GET") -> bool:
    roles = role_for_path(path, method)
    cfg = config()
    if not roles or not cfg.enabled:
        return False
    if roles == ("ui",):
        return bool(cfg.ui_token or cfg.admin_token)
    if "admin" in roles:
        return True
    return any(configured_token_for_role(role) for role in roles) or bool(cfg.admin_token)
