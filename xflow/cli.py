from __future__ import annotations

from pathlib import Path

import typer
import uvicorn

from xflow.auth.agent import check_stored_auth
from xflow.auth.playwright_login import manual_login_and_extract_tokens
from xflow.config import DEFAULT_CONFIG_PATH, load_config, write_default_config
from xflow.db import init_db
from xflow.digest import generate_digest
from xflow.pipeline import run_fetch
from xflow.server import create_app
from xflow.storage import Storage
from xflow.utils import DEFAULT_DATA_DIR, DEFAULT_PROFILE_DIR, mask_token

app = typer.Typer(help="xFlow: turn cached X/Twitter sources into RSS/JSON feeds.")
auth_app = typer.Typer(help="Manage manually extracted X auth cookies.")
app.add_typer(auth_app, name="auth")


@app.command()
def init(config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    if not config.exists():
        write_default_config(config)
        typer.echo(f"Created {config}")
    else:
        typer.echo(f"Kept existing {config}")
    loaded = load_config(config)
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    loaded.storage.profile_dir.mkdir(parents=True, exist_ok=True)
    init_db(loaded.storage.database)
    typer.echo(f"Initialized database at {loaded.storage.database}")


@app.command()
def fetch(config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    result = run_fetch(loaded, Storage(loaded.storage.database))
    typer.echo(f"Fetched {result['fetched']} tweets from {result['sources']} sources; analyzed {result['analyzed']}.")


@app.command()
def serve(config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    uvicorn.run(create_app(config), host=loaded.server.host, port=loaded.server.port)


@app.command()
def digest(
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c"),
    output: Path | None = typer.Option(None, "--output", "-o"),
) -> None:
    loaded = load_config(config)
    markdown = generate_digest(Storage(loaded.storage.database), loaded.agent.importance_threshold)
    if output:
        output.write_text(markdown, encoding="utf-8")
        typer.echo(f"Wrote digest to {output}")
    else:
        typer.echo(markdown)


@auth_app.command("login")
def auth_login(
    label: str = typer.Option(..., "--label"),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c"),
    timeout_seconds: int = typer.Option(300, "--timeout"),
) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    profile_dir = loaded.storage.profile_dir / label
    auth_token, ct0 = manual_login_and_extract_tokens(profile_dir, timeout_seconds)
    Storage(loaded.storage.database).save_auth_account(label, auth_token, ct0)
    typer.echo(f"Saved auth for {label}: auth_token={mask_token(auth_token)} ct0={mask_token(ct0)}")


@auth_app.command("check")
def auth_check(label: str = typer.Option(..., "--label"), config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    account = Storage(loaded.storage.database).get_auth_account(label)
    if not account:
        typer.echo(f"{label}: invalid - no stored account")
        raise typer.Exit(code=1)
    status = check_stored_auth(label, account["auth_token"], account["ct0"])
    typer.echo(f"{label}: {status.status} - {status.message}")
    typer.echo(f"auth_token={status.auth_token_masked} ct0={status.ct0_masked}")


@auth_app.command("list")
def auth_list(config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    accounts = Storage(loaded.storage.database).list_auth_accounts()
    if not accounts:
        typer.echo("No auth accounts stored.")
        return
    for account in accounts:
        typer.echo(
            f"{account['label']}: auth_token={mask_token(account['auth_token'])} "
            f"ct0={mask_token(account['ct0'])} updated_at={account['updated_at']}"
        )


@auth_app.command("delete")
def auth_delete(label: str = typer.Option(..., "--label"), config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c")) -> None:
    loaded = load_config(config)
    init_db(loaded.storage.database)
    deleted = Storage(loaded.storage.database).delete_auth_account(label)
    if deleted:
        typer.echo(f"Deleted auth account {label}.")
    else:
        typer.echo(f"No auth account found for {label}.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
