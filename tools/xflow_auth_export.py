from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export X auth_token/ct0 cookies for xFlow.")
    parser.add_argument("--label", required=True, help="Account label to store on the server.")
    parser.add_argument("--out", required=True, type=Path, help="Output token JSON path.")
    parser.add_argument("--timeout", type=int, default=300, help="Seconds to wait for login cookies.")
    parser.add_argument("--profile-dir", type=Path, default=Path(".xflow-auth-profile"), help="Local browser profile directory.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SystemExit("Install Playwright locally with: pip install playwright && playwright install chromium") from exc

    deadline = time.time() + args.timeout
    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            str(args.profile_dir / args.label),
            headless=False,
        )
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
