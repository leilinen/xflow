from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_BROWSER_PATHS = [
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
    Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export X auth_token/ct0 cookies for xFlow.")
    parser.add_argument("--label", required=True, help="Account label to store on the server.")
    parser.add_argument("--out", required=True, type=Path, help="Output token JSON path.")
    parser.add_argument("--timeout", type=int, default=300, help="Seconds to wait for login cookies.")
    parser.add_argument("--profile-dir", type=Path, default=Path(".xflow-auth-profile"), help="Local browser profile directory.")
    parser.add_argument("--executable-path", type=Path, help="Use an installed Chromium-based browser instead of Playwright's bundled browser.")
    return parser.parse_args()


def browser_executable_path(explicit_path: Path | None) -> Path | None:
    if explicit_path:
        return explicit_path
    return next((path for path in DEFAULT_BROWSER_PATHS if path.exists()), None)


def main() -> None:
    args = parse_args()
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SystemExit("Install Playwright locally with: pip install playwright && playwright install chromium") from exc

    deadline = time.time() + args.timeout
    executable_path = browser_executable_path(args.executable_path)
    with sync_playwright() as playwright:
        launch_options = {"headless": False}
        if executable_path:
            launch_options["executable_path"] = str(executable_path)
            print(f"Using browser executable: {executable_path}")
        context = playwright.chromium.launch_persistent_context(str(args.profile_dir / args.label), **launch_options)
        page = context.new_page()
        page.goto("https://x.com/home")
        print("Log in to X in the opened browser window. Waiting for auth_token and ct0 cookies...")
        auth_token = None
        ct0 = None
        while time.time() < deadline:
            cookies = {cookie["name"]: cookie["value"] for cookie in context.cookies("https://x.com")}
            auth_token = cookies.get("auth_token")
            ct0 = cookies.get("ct0")
            if auth_token and ct0:
                break
            time.sleep(2)
        context.close()

    if not auth_token or not ct0:
        raise SystemExit("Timed out waiting for auth_token and ct0 cookies.")

    payload = {
        "label": args.label,
        "domain": "x.com",
        "auth_token": auth_token,
        "ct0": ct0,
        "exported_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote token JSON to {args.out}. Treat this file like a password and delete it after import.")


if __name__ == "__main__":
    main()
