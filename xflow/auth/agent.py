from __future__ import annotations

from dataclasses import dataclass

from xflow.utils import mask_token


@dataclass(frozen=True)
class AuthStatus:
    label: str
    status: str
    message: str
    auth_token_masked: str = "<missing>"
    ct0_masked: str = "<missing>"


def check_stored_auth(label: str, auth_token: str | None, ct0: str | None) -> AuthStatus:
    if not auth_token or not ct0:
        return AuthStatus(label, "invalid", "Missing auth_token or ct0.")
    if len(auth_token) < 8 or len(ct0) < 8:
        return AuthStatus(label, "invalid", "Stored cookies are too short to be valid.", mask_token(auth_token), mask_token(ct0))
    return AuthStatus(
        label,
        "unknown",
        "Stored cookies are present. Live X validation is intentionally not performed in v1.",
        mask_token(auth_token),
        mask_token(ct0),
    )
