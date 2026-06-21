use crate::channel::telegram;
use crate::config::{load_config, write_default_config};
use crate::digest;
use crate::fetch::auth;
use crate::server;
use crate::storage;
use crate::storage::db;
use crate::utils::{mask_token, DEFAULT_CONFIG_PATH};
use crate::worker;
use crate::worker::pipeline;
use clap::{Args, Parser, Subcommand};
use sqlx::PgPool;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "xflow",
    about = "Turn cached X/Twitter sources into RSS/JSON feeds."
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(ConfigOpt),
    Fetch(ConfigOpt),
    Serve(ConfigOpt),
    Worker(ConfigOpt),
    Backfill(BackfillArgs),
    Digest(DigestArgs),
    Telegram {
        #[command(subcommand)]
        command: TelegramCommand,
    },
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
}

#[derive(Debug, Args, Clone)]
struct ConfigOpt {
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
}

#[derive(Debug, Args)]
struct DigestArgs {
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct BackfillArgs {
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[arg(long)]
    username: String,
    #[arg(long, default_value_t = 0)]
    max_pages: usize,
    #[arg(long, default_value_t = 2)]
    page_delay: u64,
    /// Stop when tweets are older than this (e.g. "7d", "30d", "12h")
    #[arg(long)]
    since: Option<String>,
}

#[derive(Debug, Subcommand)]
enum TelegramCommand {
    Send(TelegramSendArgs),
    Commands {
        #[command(subcommand)]
        command: TelegramCommandsCommand,
    },
}

#[derive(Debug, Args)]
struct TelegramSendArgs {
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[arg(long, default_value_t = 100)]
    limit: i64,
}

#[derive(Debug, Subcommand)]
enum TelegramCommandsCommand {
    Set(ConfigOpt),
    List(ConfigOpt),
    Clear(ConfigOpt),
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    Import(AuthImportArgs),
    List(ConfigOpt),
    Check(AuthLabelArgs),
    Delete(AuthLabelArgs),
}

#[derive(Debug, Args)]
struct AuthImportArgs {
    token_json: Option<PathBuf>,
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[arg(long)]
    label: Option<String>,
    #[arg(long)]
    auth_token: Option<String>,
    #[arg(long)]
    ct0: Option<String>,
}

#[derive(Debug, Args)]
struct AuthLabelArgs {
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[arg(long)]
    label: String,
    #[arg(long)]
    live: bool,
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init(args) => init(args).await,
        Command::Fetch(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            let result = pipeline::run_fetch(&config, &pool).await?;
            println!(
                "Fetched {} tweets from {} sources; analyzed {}; failed {}.",
                result.fetched, result.sources, result.analyzed, result.failed
            );
            if result.failed > 0 {
                for error in &result.errors {
                    eprintln!(
                        "{}:{} failed: {}",
                        error.source_type, error.source_value, error.message
                    );
                }
                anyhow::bail!("fetch completed with {} failed sources", result.failed);
            }
            Ok(())
        }
        Command::Serve(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            server::serve(config, pool).await
        }
        Command::Worker(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            println!(
                "Starting worker with interval {}s.",
                config.fetch.interval_seconds
            );
            worker::run_forever(config, pool).await
        }
        Command::Backfill(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            let since = match &args.since {
                Some(s) => Some(parse_duration(s)?),
                None => None,
            };
            let result = crate::fetch::backfill_user(
                &config,
                &pool,
                &args.username,
                args.max_pages,
                args.page_delay,
                since,
            )
            .await?;
            println!(
                "Backfill @{} complete: {} total, {} new, {} existing, {} pages.",
                args.username, result.total, result.new, result.duplicate, result.pages
            );
            Ok(())
        }
        Command::Digest(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            let markdown =
                digest::generate_digest(&pool, config.agent.importance_threshold).await?;
            if let Some(output) = args.output {
                std::fs::write(&output, markdown)?;
                println!("Wrote digest to {}", output.display());
            } else {
                println!("{markdown}");
            }
            Ok(())
        }
        Command::Telegram { command } => match command {
            TelegramCommand::Send(args) => {
                let (config, pool) = configured_pool(&args.config).await?;
                let result = telegram::send_undelivered(
                    &pool,
                    &config.telegram,
                    &config.comments,
                    &config.translation,
                    args.limit,
                    config.fetch.max_delivery_retries,
                )
                .await?;
                println!(
                    "Telegram delivery: sent {}, failed {}, skipped {}.",
                    result.sent, result.failed, result.skipped
                );
                Ok(())
            }
            TelegramCommand::Commands { command } => telegram_commands_command(command).await,
        },
        Command::Auth { command } => auth_command(command).await,
    }
}

async fn telegram_commands_command(command: TelegramCommandsCommand) -> anyhow::Result<()> {
    match command {
        TelegramCommandsCommand::Set(args) => {
            let config = load_config(&args.config)?;
            let commands = telegram::set_bot_commands(&config.telegram).await?;
            println!("Registered {} Telegram commands.", commands.len());
            for command in commands {
                println!("/{} - {}", command.command, command.description);
            }
            Ok(())
        }
        TelegramCommandsCommand::List(args) => {
            let config = load_config(&args.config)?;
            let commands = telegram::list_bot_commands(&config.telegram).await?;
            if commands.is_empty() {
                println!("No Telegram commands registered.");
            } else {
                for command in commands {
                    println!("/{} - {}", command.command, command.description);
                }
            }
            Ok(())
        }
        TelegramCommandsCommand::Clear(args) => {
            let config = load_config(&args.config)?;
            telegram::clear_bot_commands(&config.telegram).await?;
            println!("Cleared Telegram commands.");
            Ok(())
        }
    }
}

async fn init(args: ConfigOpt) -> anyhow::Result<()> {
    if !args.config.exists() {
        write_default_config(&args.config)?;
        println!("Created {}", args.config.display());
    } else {
        println!("Kept existing {}", args.config.display());
    }
    let config = load_config(&args.config)?;
    let pool = db::connect(&config.storage.database_url).await?;
    db::init_db(&pool).await?;
    println!("Initialized database at {}", config.storage.database_url);
    Ok(())
}

async fn configured_pool(config_path: &Path) -> anyhow::Result<(crate::config::AppConfig, PgPool)> {
    let config = load_config(config_path)?;
    let pool = db::connect(&config.storage.database_url).await?;
    db::init_db(&pool).await?;
    Ok((config, pool))
}

/// Parse a human duration string like "7d", "30d", "12h" into hours.
fn parse_duration(input: &str) -> anyhow::Result<chrono::Duration> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("duration cannot be empty");
    }
    let (num_str, unit) = if input.ends_with('d') {
        (&input[..input.len() - 1], 'd')
    } else if input.ends_with('h') {
        (&input[..input.len() - 1], 'h')
    } else {
        anyhow::bail!("duration must end with 'd' (days) or 'h' (hours), e.g. \"7d\" or \"12h\"");
    };
    let value: i64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number in duration: {num_str}"))?;
    match unit {
        'd' => Ok(chrono::Duration::days(value)),
        'h' => Ok(chrono::Duration::hours(value)),
        _ => unreachable!(),
    }
}

async fn auth_command(command: AuthCommand) -> anyhow::Result<()> {
    match command {
        AuthCommand::Import(args) => {
            let (_config, pool) = configured_pool(&args.config).await?;
            if let Some(path) = args.token_json {
                let token = auth::import_token_json(&pool, &path).await?;
                println!(
                    "Imported auth for {}: auth_token={} ct0={}",
                    token.label,
                    mask_token(&token.auth_token),
                    mask_token(&token.ct0)
                );
            } else {
                let label = args.label.ok_or_else(|| {
                    anyhow::anyhow!("--label is required when no token JSON is provided")
                })?;
                let auth_token = args.auth_token.ok_or_else(|| {
                    anyhow::anyhow!("--auth-token is required when no token JSON is provided")
                })?;
                let ct0 = args.ct0.ok_or_else(|| {
                    anyhow::anyhow!("--ct0 is required when no token JSON is provided")
                })?;
                let token = auth::import_token_values(&pool, label, auth_token, ct0).await?;
                println!(
                    "Imported auth for {}: auth_token={} ct0={}",
                    token.label,
                    mask_token(&token.auth_token),
                    mask_token(&token.ct0)
                );
            }
            Ok(())
        }
        AuthCommand::List(args) => {
            let (_config, pool) = configured_pool(&args.config).await?;
            let accounts = storage::list_auth_accounts(&pool).await?;
            if accounts.is_empty() {
                println!("No auth accounts stored.");
            }
            for account in accounts {
                println!(
                    "{}: domain={} auth_token={} ct0={} status={} updated_at={}",
                    account.label,
                    account.domain,
                    account.auth_token_masked,
                    account.ct0_masked,
                    account.status,
                    account.updated_at
                );
            }
            Ok(())
        }
        AuthCommand::Check(args) => {
            let (config, pool) = configured_pool(&args.config).await?;
            let account = auth::check_account(&pool, &args.label).await?;
            if args.live {
                let result = auth::check_account_live(&pool, &config).await?;
                let status_msg = if result.ok {
                    "ok".to_string()
                } else {
                    result.error.unwrap_or_else(|| "unknown error".to_string())
                };
                println!(
                    "{}: {} - live check: {status_msg}",
                    account.label, account.status,
                );
            } else {
                println!(
                    "{}: {} - stored token shape looks present",
                    account.label, account.status
                );
            }
            println!(
                "auth_token={} ct0={}",
                account.auth_token_masked, account.ct0_masked
            );
            Ok(())
        }
        AuthCommand::Delete(args) => {
            let (_config, pool) = configured_pool(&args.config).await?;
            if storage::delete_auth_account(&pool, &args.label).await? {
                println!("Deleted auth account {}.", args.label);
            } else {
                anyhow::bail!("No auth account found for {}.", args.label);
            }
            Ok(())
        }
    }
}
