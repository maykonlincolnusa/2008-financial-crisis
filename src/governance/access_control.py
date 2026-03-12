from __future__ import annotations

from dataclasses import dataclass

from src.utils.logging_setup import setup_logging, log_event


@dataclass(frozen=True)
class Role:
    name: str


def check_role(user_role: Role, required: Role) -> bool:
    """Simple role-based access control check."""
    logger = setup_logging("governance.rbac")
    ok = user_role.name == required.name
    log_event(logger, "rbac_check", user=user_role.name, required=required.name, ok=ok)
    return ok
