from __future__ import annotations

import time
from pathlib import Path


def manual_login_and_extract_tokens(profile_dir: Path, timeout_seconds: int = 300) -> tuple[str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Playwright is not installed. Install dependencies and run `playwright install chromium`.") from exc

    profile_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile_dir), headless=False)
        page = context.new_page()
        page.goto("https://x.com/home")
        auth_token = ""
        ct0 = ""
        try:
            while time.monotonic() < deadline:
                cookies = {cookie["name"]: cookie["value"] for cookie in context.cookies()}
                auth_token = cookies.get("auth_token", "")
                ct0 = cookies.get("ct0", "")
                if auth_token and ct0:
                    return auth_token, ct0
                page.wait_for_timeout(1000)
        finally:
            context.close()
    raise TimeoutError("Timed out waiting for auth_token and ct0 cookies after manual login.")
