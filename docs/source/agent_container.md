# Agent Container (Agentic Coding)

This page describes the limited-access agent container in `.agentcontainer/` — how it differs from the human dev container (`.devcontainer/`), how to build and run it, and how to use it for agentic coding with `opencode`, `claude`, and `codex`.

## When to use which container

|  | `.devcontainer/` (human) | `.agentcontainer/` (AI agents) |
|---|---|---|
| User | `vscode`, with passwordless `sudo` | `agent` (UID/GID 1000), **no sudo** |
| Purpose | Full-access local development | Least-privilege sandbox for AI coding agents |
| Workspace | `/workspaces/<repo>` + Postgres sidecar (`db` service) | `/workspaces` (repo root mounted from `..:/workspaces`) |
| Network/ports | Postgres sidecar, `forwardPorts` available | No published ports, no host network, no Docker socket |
| Hardening | None (dev convenience) | `read_only: true`, `cap_drop: [ALL]`, `no-new-privileges:true`, `init: true`, ephemeral `tmpfs` for `/tmp` and `/home/agent` |
| Secrets | `OP_SERVICE_ACCOUNT_TOKEN` via `remoteEnv`, `op inject` into `.dlt/secrets.toml` / `.env.databricks` | Docker secrets from `../.secrets/` files, exported by the entrypoint (see below) |
| Agent CLIs | Not preinstalled | `opencode-ai`, `@anthropic-ai/claude-code`, `@openai/codex` (global npm installs) plus `gh`, `op`, `uv` |

## What's inside

- **Base**: `python:3.13-slim` (`Dockerfile`), `WORKDIR /workspaces`, `ENTRYPOINT [entrypoint.sh]`, default `CMD sleep infinity`.
- **System packages**: `git`, `curl`, `unzip`, `ca-certificates`, Node 22 (via nodesource), `openssh-client` (SSH signing only, keys never copied in).
- **CLIs**: GitHub CLI (`gh`), 1Password CLI (`op`, pinned via `ARG OP_VERSION=2.32.0`), AI CLIs (`opencode-ai`, `claude-code`, `codex`), and `uv` (dlt/dbt workflows).
- **Compose** (`docker-compose.yml`, image `personal-reporting-agentcontainer:local`):
  - Binds the repo root (`..:/workspaces:cached`) so edits land directly in your checkout.
  - Forwards the host SSH agent (`/run/host-services/ssh-auth.sock`, read-only) — no private keys enter the container.
  - Mounts host agent configs read-only: `~/.config/opencode`, `~/.claude` → `/home/agent/.host-claude`, `~/.codex` → `/home/agent/.host-codex`, `~/.agents` → `/home/agent/.agents`.
  - Consumes three Docker secrets from `../.secrets/`: `opencode_api_key`, `github_token`, `op_service_account_token`.
  - Sets non-sensitive env: `ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL` (opencode zen proxy), `ANTHROPIC_MODEL=qwen-3.8-flash`, `GITHUB_NAME` / `GITHUB_EMAIL`, `DBT_TARGET`.
- **Entrypoint** (`scripts/entrypoint.sh`) runs on every start:
  1. Reads `/run/secrets/*` into env (`OPENCODE_API_KEY` + `ANTHROPIC_API_KEY` + `OPENAI_API_KEY`, `GITHUB_TOKEN`, `OP_SERVICE_ACCOUNT_TOKEN`), persists them to `~/.secrets_env`, and wires `~/.bashrc` to source it.
  2. Copies **config files only** (no session/state) from the host mounts into the writable home: claude `settings.json` / `CLAUDE.md` / `keybindings.json` plus `themes rules skills agents workflows output-styles` dirs; codex `config.toml` / `hooks.json` / `AGENTS.md` plus `*.config.toml` profiles. OpenCode needs no copy (config stays on its read-only mount, state lives in `~/.local/share/opencode/`); dlthub skills stay on the `~/.agents` mount.
  3. Configures git identity from `GITHUB_NAME` / `GITHUB_EMAIL` and, when the forwarded SSH agent holds a key, enables SSH commit signing (`gpg.format ssh`, `user.signingkey key::<first-key>`).
- **Devcontainer descriptor** (`devcontainer.json`, `"name": "AI Agents (Limited Access)"`) reuses the compose `agent` service (`workspaceFolder: /workspaces`, `remoteUser: agent`, bash as default terminal) and forwards `OPENCODE_API_KEY` plus the `ANTHROPIC_*` / `OPENAI_*` zen-proxy vars from the host environment.

Note: `/home/agent` is an ephemeral `tmpfs` — agent home state (copied configs, `~/.secrets_env`, tool caches) is rebuilt by the entrypoint on every start. The repo checkout (`/workspaces`, including `.venv/`) persists via the bind mount.

## Prerequisites

- Docker (Desktop on macOS/Windows — required for the `/run/host-services/ssh-auth.sock` SSH-agent bridge) with Compose v2.
- Three secret files in `.secrets/` (gitignored, never committed):
  - `.secrets/opencode_api_key`
  - `.secrets/github_token`
  - `.secrets/op_service_account_token`
- Optional but recommended: host SSH agent running with your signing key, host configs in `~/.config/opencode`, `~/.claude`, `~/.codex`, `~/.agents`, and `GITHUB_NAME` / `GITHUB_EMAIL` exported for commit attribution.

## Build and run

```bash
cd .agentcontainer

# Build the image
docker compose build

# Start the sandbox (detached, sleeps forever)
GITHUB_NAME="Your Name" GITHUB_EMAIL="you@example.com" docker compose up -d

# Open a shell inside it
docker compose exec agent bash

# Tear down (home state is ephemeral and discarded)
docker compose down
```

VSCode alternative: open the repo with the `.agentcontainer/devcontainer.json` configuration ("Reopen in Container" pointed at `.agentcontainer`), which attaches as `agent` with the same compose service, mounts, and env.

## Use for agentic coding

1. **Shell in**, then confirm the tooling:
   ```bash
   opencode --version && claude --version && codex --version
   gh --version && op --version && uv --version
   env | grep -E '^(OPENCODE_API_KEY|GITHUB_TOKEN|OP_SERVICE_ACCOUNT_TOKEN|ANTHROPIC_BASE_URL|OPENAI_BASE_URL)' | sed 's/=.*/=<set>/'
   ssh-add -l   # should list the forwarded host key; no keys stored in the image
   ```
2. **Sync project deps** (persists into the bind-mounted workspace):
   ```bash
   uv sync
   # or: make install
   ```
3. **Inject data-warehouse credentials** when a task needs them (1Password token is already in env via the secret):
   ```bash
   make inject
   # writes .dlt/secrets.toml and .env.databricks (both gitignored)
   ```
4. **Run an agent** from `/workspaces`:
   ```bash
   opencode run "your task here"
   claude -p "your task here"
   codex exec "your task here"
   ```
   Project agent wiring the container picks up automatically:
   - `opencode.json` — `dlt-workspace-mcp` MCP server (`uv run dlthub ai mcp --stdio`).
   - `.codex/config.toml` — same `dlt-workspace-mcp` MCP server for Codex.
   - `.agents/skills/` + `AGENTS.md` / `CLAUDE.md` — repo instructions and dlthub workflow skills; host-level skills arrive via the `~/.agents` mount.
5. **Validate before finishing**: `make test-local` (Python units), `uv run dbt build --project-dir dbt --profiles-dir dbt --target mock` (dbt on DuckDB fixtures), `uv run dbt lint --project-dir dbt --profiles-dir dbt --target mock` (SQL lint). See {doc}`testing_ci` for details.
6. **Commit from inside**: git identity and SSH signing are already configured by the entrypoint; push over HTTPS with `GITHUB_TOKEN` or over SSH via the forwarded agent. Keep commits on feature branches — never commit secrets (`.secrets/`, `*/secrets.toml`, `.env.databricks`) or direct to `main`.

## Preserve OpenCode session history

The agent home directory is an ephemeral `tmpfs`, so OpenCode sessions under
`~/.local/share/opencode/` are lost when the sandbox is removed. Export sessions
to durable storage before teardown and import them after starting a new sandbox.
OpenCode's [CLI documentation](https://opencode.ai/docs/cli/) describes the
`session list`, `export`, and `import` commands.

These examples require `jq` and the Google Cloud CLI with permission to access
the bucket. They are not installed by the agent image; install them on the
machine running the commands using the [jq installation
instructions](https://jqlang.org/download/) and Google's [gcloud CLI
installation guide](https://cloud.google.com/sdk/docs/install).

The following example stops on command errors, verifies that the export is
non-empty, and removes the local copy after upload. Replace the bucket name with
a bucket to which the operator has access:

```bash
set -Eeuo pipefail

bucket="gs://opencode-history"
session_id="$(opencode session list --format json -n 1 | jq -r '.[0].id')"
[[ -n "$session_id" && "$session_id" != "null" ]] || {
    printf 'No OpenCode session found\n' >&2
    exit 1
}

export_file="${session_id}.json"
cleanup() { rm -f -- "$export_file"; }
trap cleanup EXIT

opencode export "$session_id" > "$export_file"
[[ -s "$export_file" ]] || {
    printf 'OpenCode export is empty: %s\n' "$export_file" >&2
    exit 1
}
gcloud storage cp "$export_file" "$bucket/$export_file"
```

After creating the replacement sandbox, run the restore commands from the
project directory where the session should be available:

```bash
set -Eeuo pipefail

bucket="gs://opencode-history"
session_id="ses_example"
export_file="${session_id}.json"
trap 'rm -f -- "$export_file"' EXIT

gcloud storage cp "$bucket/$export_file" "$export_file"
[[ -s "$export_file" ]] || {
    printf 'Downloaded session is empty: %s\n' "$export_file" >&2
    exit 1
}
opencode import "$export_file"
```

Treat exported JSON as sensitive: it contains the session transcript and may
include file contents or other workspace data. Restrict the bucket's IAM access
and retention policy, and do not commit exports to the repository. See Google's
[Cloud Storage upload](https://cloud.google.com/storage/docs/uploading-objects)
and [download](https://cloud.google.com/storage/docs/downloading-objects)
documentation for the `gcloud storage cp` behavior.

## Troubleshooting

- **Missing secrets**: `docker compose` fails if a file under `../.secrets/` is absent — create all three files first.
- **Empty `ssh-add -l`**: the host SSH agent isn't forwarded (common outside Docker Desktop). Commits still work, but SSH signing is skipped.
- **Config not picked up**: the entrypoint copies only the listed config filenames/dirs — check spelling against `scripts/entrypoint.sh`, then restart the container.
- **Lost home state after restart**: expected (`tmpfs`). Re-runs of the entrypoint restore secrets, configs, and git identity; project files in `/workspaces` are unaffected.
- **Stale image after Dockerfile edits**: rerun `docker compose build` before `up`.
