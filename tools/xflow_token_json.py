from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create xFlow token JSON from X cookies.")
    parser.add_argument("--label", default="account1", help="Account label to store in xFlow.")
    parser.add_argument("--auth-token", required=True, help="X auth_token cookie value.")
    parser.add_argument("--ct0", required=True, help="X ct0 cookie value.")
    parser.add_argument("--out", type=Path, default=Path("/tmp/xflow-token.json"), help="Output token JSON path.")
    return parser.parse_args()


def validate_token_shape(auth_token: str, ct0: str) -> None:
    if len(auth_token) < 8:
        raise SystemExit("auth_token is too short.")
    if len(ct0) < 4:
        raise SystemExit("ct0 is too short.")


def main() -> None:
    args = parse_args()
    validate_token_shape(args.auth_token, args.ct0)

    payload = {
        "label": args.label,
        "domain": "x.com",
        "auth_token": args.auth_token,
        "ct0": args.ct0,
        "exported_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(args.out, 0o600)
    print(f"Wrote token JSON for {args.label} to {args.out}. Delete it after import.")


if __name__ == "__main__":
    main()
