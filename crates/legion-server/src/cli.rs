//! REST client commands for the `legion` binary.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use base64::Engine;
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};

#[derive(Debug, Parser)]
#[command(name = "legion", version, about = "Legion durable functions server and client")]
pub struct Cli {
    /// REST API base URL used by client commands.
    #[arg(long, env = "LEGION_URL", default_value = "http://127.0.0.1:8080", global = true)]
    pub url: String,

    /// API key sent as a bearer token.
    #[arg(long, env = "LEGION_API_KEY", global = true, hide_env_values = true)]
    pub api_key: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the Legion daemon (the default when no command is given).
    Serve,
    /// Check API health.
    Health,
    /// Inspect cluster state.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Manage durable agent sessions.
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Deploy and manage functions.
    Deploy {
        #[command(subcommand)]
        command: DeployCommand,
    },
    /// Invoke a deployed function. JSON is read from --json or stdin.
    Call(CallArgs),
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    Health,
    Peers,
}

#[derive(Debug, Subcommand)]
pub enum SessionCommand {
    List(ListArgs),
    New(NewSessionArgs),
    Status { id: String },
    Send { id: String, message: String },
    History { id: String },
    Stream { id: String, message: Option<String> },
    /// Resolve a dangling tool call after a crash.
    Reconcile {
        id: String,
        #[arg(long, value_enum)]
        action: ReconcileActionArg,
    },
}

#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(long)]
    pub status: Option<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
}

#[derive(Debug, Args)]
pub struct NewSessionArgs {
    #[arg(long, default_value = "anthropic/claude-haiku-3-5")]
    pub model: String,
    #[arg(long)]
    pub system_prompt: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    /// Deploy source or a WASM module in one operation.
    Push(DeployArgs),
    /// Register an existing CAS artifact as a function.
    Register(RegisterArgs),
    /// Set an artifact's routing weight in basis points.
    Route(RouteArgs),
    /// Promote an artifact to receive all traffic.
    Promote(PromoteArgs),
    /// List deployed functions.
    List,
    /// Remove a deployed function.
    Delete { name: String },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RuntimeArg {
    Bun,
    Wasm,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ReconcileActionArg {
    Skip,
    Retry,
}

#[derive(Debug, Args)]
pub struct DeployArgs {
    pub name: String,
    pub path: PathBuf,
    #[arg(long, value_enum)]
    pub runtime: RuntimeArg,
    #[arg(long)]
    pub description: Option<String>,
    /// JSON Schema file describing function parameters.
    #[arg(long)]
    pub schema: Option<PathBuf>,
    #[arg(long)]
    pub idempotent: bool,
    /// Environment variable in KEY=VALUE form; may be repeated.
    #[arg(long = "env", value_parser = parse_env)]
    pub env: Vec<(String, String)>,
}

#[derive(Debug, Args)]
pub struct RegisterArgs {
    pub name: String,
    pub artifact_cid: String,
    #[arg(long, value_enum)]
    pub runtime: RuntimeArg,
    #[arg(long)]
    pub description: Option<String>,
    #[arg(long)]
    pub schema: Option<PathBuf>,
    #[arg(long)]
    pub idempotent: bool,
    /// Environment variable in KEY=VALUE form; may be repeated.
    #[arg(long = "env", value_parser = parse_env)]
    pub env: Vec<(String, String)>,
}

#[derive(Debug, Args)]
pub struct RouteArgs {
    pub name: String,
    pub artifact_cid: String,
    /// Traffic weight in basis points (0-10000).
    #[arg(long, default_value_t = 10_000, value_parser = clap::value_parser!(u16).range(0..=10_000))]
    pub weight: u16,
}

#[derive(Debug, Args)]
pub struct PromoteArgs {
    pub name: String,
    pub artifact_cid: String,
}

#[derive(Debug, Args)]
pub struct CallArgs {
    pub name: String,
    /// JSON arguments. If omitted, JSON is read from stdin; empty stdin becomes {}.
    #[arg(long)]
    pub json: Option<String>,
}

pub async fn run(cli: Cli) -> Result<()> {
    let command = cli.command.as_ref().context("missing client command")?;
    let client = ApiClient::new(&cli.url, cli.api_key.as_deref())?;

    match command {
        Command::Serve => unreachable!("serve is handled by main"),
        Command::Health => print_json(client.json(Method::GET, "/health", None).await?),
        Command::Cluster { command } => match command {
            ClusterCommand::Health => print_json(client.json(Method::GET, "/health", None).await?),
            ClusterCommand::Peers => print_json(client.json(Method::GET, "/cluster/peers", None).await?),
        },
        Command::Session { command } => run_session(&client, command).await?,
        Command::Deploy { command } => run_deploy(&client, command).await?,
        Command::Call(args) => {
            let input = read_json_input(args.json.as_deref())?;
            let output = client.json(
                Method::POST,
                &format!("/functions/{}/invoke", args.name),
                Some(input),
            ).await?;
            print_json(output.get("output").cloned().unwrap_or(output));
        }
    }
    Ok(())
}

async fn run_session(client: &ApiClient, command: &SessionCommand) -> Result<()> {
    match command {
        SessionCommand::List(args) => {
            let mut query = vec![
                ("limit", args.limit.to_string()),
                ("offset", args.offset.to_string()),
            ];
            if let Some(status) = &args.status {
                query.push(("status", status.clone()));
            }
            print_json(client.json_query("/sessions", &query).await?);
        }
        SessionCommand::New(args) => {
            let body = json!({
                "model": args.model,
                "system_prompt": args.system_prompt,
            });
            print_json(client.json(Method::POST, "/sessions", Some(body)).await?);
        }
        SessionCommand::Status { id } => {
            print_json(client.json(Method::GET, &format!("/sessions/{id}"), None).await?);
        }
        SessionCommand::Send { id, message } => {
            print_json(client.json(
                Method::POST,
                &format!("/sessions/{id}/messages"),
                Some(json!({ "content": message })),
            ).await?);
        }
        SessionCommand::History { id } => {
            print_json(client.json(Method::GET, &format!("/sessions/{id}/log"), None).await?);
        }
        SessionCommand::Stream { id, message } => {
            client.stream_session(id, message.as_deref()).await?;
        }
        SessionCommand::Reconcile { id, action } => {
            let action = match action {
                ReconcileActionArg::Skip => "skip",
                ReconcileActionArg::Retry => "retry",
            };
            print_json(client.json(
                Method::POST,
                &format!("/sessions/{id}/reconcile"),
                Some(json!({ "action": action })),
            ).await?);
        }
    }
    Ok(())
}

async fn run_deploy(client: &ApiClient, command: &DeployCommand) -> Result<()> {
    match command {
        DeployCommand::List => print_json(client.json(Method::GET, "/functions", None).await?),
        DeployCommand::Delete { name } => {
            print_json(client.json(Method::DELETE, &format!("/functions/{name}"), None).await?);
        }
        DeployCommand::Register(args) => {
            let parameters = read_schema(args.schema.as_ref())?;
            let runtime = match args.runtime {
                RuntimeArg::Bun => "bun",
                RuntimeArg::Wasm => "wasm",
            };
            print_json(client.json(Method::POST, "/deploy/register", Some(json!({
                "name": args.name,
                "artifact_cid": args.artifact_cid,
                "runtime": runtime,
                "description": args.description,
                "parameters": parameters,
                "idempotent": args.idempotent,
                "env": args.env.iter().cloned().collect::<std::collections::BTreeMap<_, _>>(),
            }))).await?);
        }
        DeployCommand::Route(args) => {
            print_json(client.json(Method::POST, "/deploy/route", Some(json!({
                "name": args.name,
                "artifact_cid": args.artifact_cid,
                "weight": args.weight,
            }))).await?);
        }
        DeployCommand::Promote(args) => {
            print_json(client.json(Method::POST, "/deploy/promote", Some(json!({
                "name": args.name,
                "artifact_cid": args.artifact_cid,
            }))).await?);
        }
        DeployCommand::Push(args) => {
            let parameters = read_schema(args.schema.as_ref())?;
            let artifact = std::fs::read(&args.path)
                .with_context(|| format!("read artifact {}", args.path.display()))?;
            let runtime = match args.runtime {
                RuntimeArg::Bun => "bun",
                RuntimeArg::Wasm => "wasm",
            };
            let mut body = json!({
                "name": args.name,
                "runtime": runtime,
                "description": args.description,
                "parameters": parameters,
                "idempotent": args.idempotent,
                "env": args.env.iter().cloned().collect::<std::collections::BTreeMap<_, _>>(),
            });
            match args.runtime {
                RuntimeArg::Bun => {
                    body["code"] = Value::String(String::from_utf8(artifact)
                        .context("Bun artifact is not UTF-8")?);
                }
                RuntimeArg::Wasm => {
                    body["wasm_b64"] = Value::String(
                        base64::engine::general_purpose::STANDARD.encode(artifact),
                    );
                }
            }
            print_json(client.json(Method::POST, "/functions", Some(body)).await?);
        }
    }
    Ok(())
}

fn parse_env(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "environment variables must use KEY=VALUE".to_string())?;
    if name.is_empty()
        || !name.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphabetic() || (index > 0 && ch.is_ascii_digit())
        })
    {
        return Err("environment variable name must match [A-Za-z_][A-Za-z0-9_]*".into());
    }
    Ok((name.into(), value.into()))
}

fn read_schema(path: Option<&PathBuf>) -> Result<Value> {
    match path {
        Some(path) => serde_json::from_slice(&std::fs::read(path)
            .with_context(|| format!("read schema {}", path.display()))?)
            .with_context(|| format!("parse schema {}", path.display())),
        None => Ok(json!({ "type": "object", "properties": {} })),
    }
}

fn read_json_input(argument: Option<&str>) -> Result<Value> {
    let text = match argument {
        Some(value) => value.to_owned(),
        None => {
            use std::io::Read;
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            input
        }
    };
    if text.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(&text).context("parse JSON input")
}

fn print_json(value: Value) {
    use std::io::Write;

    let output = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    if let Err(error) = writeln!(std::io::stdout().lock(), "{output}") {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            eprintln!("failed writing output: {error}");
        }
    }
}

struct ApiClient {
    base: String,
    client: Client,
    api_key: Option<String>,
}

impl ApiClient {
    fn new(base: &str, api_key: Option<&str>) -> Result<Self> {
        let base = base.trim_end_matches('/').to_owned();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            bail!("LEGION_URL must start with http:// or https://");
        }
        Ok(Self {
            base,
            client: Client::builder().build()?,
            api_key: api_key.map(str::to_owned),
        })
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.client.request(method, format!("{}{}", self.base, path));
        match &self.api_key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    }

    async fn json(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        decode_response(response).await
    }

    async fn json_query(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let response = self.request(Method::GET, path).query(query).send().await?;
        decode_response(response).await
    }

    async fn stream_session(&self, id: &str, message: Option<&str>) -> Result<()> {
        let mut request = self.request(Method::GET, &format!("/sessions/{id}/stream"));
        if let Some(message) = message {
            request = request.query(&[("message", message)]);
        }
        let mut response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("API {status}: {body}");
        }
        while let Some(chunk) = response.chunk().await? {
            print!("{}", String::from_utf8_lossy(&chunk));
        }
        Ok(())
    }
}

async fn decode_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let text = response.text().await?;
    let value = serde_json::from_str(&text).unwrap_or_else(|_| Value::String(text.clone()));
    if status.is_success() {
        Ok(value)
    } else {
        let reason = if status == StatusCode::UNAUTHORIZED {
            "authentication failed".to_owned()
        } else {
            value.get("error").and_then(Value::as_str).unwrap_or(&text).to_owned()
        };
        bail!("API {status}: {reason}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_is_empty_object() {
        assert_eq!(read_json_input(Some("  ")).unwrap(), json!({}));
    }

    #[test]
    fn parses_inline_json() {
        assert_eq!(read_json_input(Some(r#"{"n": 7}"#)).unwrap(), json!({"n": 7}));
    }

    #[test]
    fn rejects_non_http_url() {
        assert!(ApiClient::new("file:///tmp/socket", None).is_err());
    }

    #[test]
    fn parses_deployment_control_commands() {
        assert!(matches!(
            Cli::try_parse_from(["legion", "deploy", "register", "hello", "cid", "--runtime", "bun"])
                .unwrap()
                .command,
            Some(Command::Deploy { command: DeployCommand::Register(_) })
        ));
        assert!(matches!(
            Cli::try_parse_from(["legion", "deploy", "route", "hello", "cid", "--weight", "2500"])
                .unwrap()
                .command,
            Some(Command::Deploy { command: DeployCommand::Route(RouteArgs { weight: 2500, .. }) })
        ));
        assert!(Cli::try_parse_from([
            "legion", "deploy", "route", "hello", "cid", "--weight", "10001"
        ]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["legion", "deploy", "promote", "hello", "cid"])
                .unwrap()
                .command,
            Some(Command::Deploy { command: DeployCommand::Promote(_) })
        ));
    }
}
