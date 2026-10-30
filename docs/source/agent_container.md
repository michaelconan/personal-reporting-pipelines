# Agent Development Environments

There are two supported agent environments. The preferred option is Docker AI
Sandbox configured by the repository-root `sbxenv.yaml`. The second option is
the restricted **agent devcontainer** at `.devcontainer/agent/`.

Docker AI Sandbox is preferred because it provides:

- Stronger isolation than the devcontainer: each sandbox is a microVM with its
  own filesystem, Docker daemon, and network policy. Agent-installed packages,
  images, and containers stay inside the sandbox.
- Least-privilege GitHub access: the agent authenticates as the machine user,
  never with your personal token. The machine user has access only to the
  target repo.
- Repeatable policy: global network presets (`Balanced` / `Locked Down`) plus
  per-host allow rules via `sbx policy`, instead of hand-maintained Compose
  hardening.

Official docs: `https://docs.docker.com/ai/sandboxes/`.

## 1. Machine user (one-time setup)

Create a dedicated GitHub machine user (a separate GitHub account used only
by agents, e.g. `YOUR-MACHINE-USER`) and grant it the minimum needed on this
repo:

1. Add it as a collaborator on the target repo only (Settings → Collaborators
   and teams) with **Write** (not Admin/Maintain). No org-wide roles.
2. Create a **fine-grained PAT** logged in as the machine user, scoped to the
   single repo, short expiry, only:
   `Contents: read/write`, `Pull requests: read/write`, `Metadata: read`.
3. Store that PAT where `sbx` can resolve it without committing it — e.g. in
   `gh auth` as the machine user, in 1Password (`op://...`), or another vault.
   Never commit it, bake it into an image/kit, or reuse your personal token.

Git identity inside sandboxes uses the machine user (`user.name` /
`user.email`), so agent commits and PRs are attributable to it.

## 2. `sbx` setup

Install and sign in (see `https://docs.docker.com/ai/sandboxes/install/`):

```bash
# macOS
brew trust docker/tap && brew install docker/tap/sbx
# Windows (current user)
winget install -h Docker.sbx
# Ubuntu (sbx only, no Engine)
curl -fsSL https://get.docker.com | sudo REPO_ONLY=1 sh
sudo apt install docker-sbx

sbx login
```

Local sandboxes need virtualization (Apple Silicon on macOS 14+, Hypervisor
Platform on Windows 11, KVM + `kvm` group on Ubuntu 24.04+). Cloud sandboxes
(`sbx --cloud`) are an option where local virtualization is unavailable.

Authenticate the agent and GitHub (see
`https://docs.docker.com/ai/sandboxes/get-started/`):

```bash
# Model provider, e.g. API key via sbx vault (or /login inside for Claude subscriptions)
sbx secret set anthropic --command 'op read "op://Private/Anthropic/api-key"'

# GitHub as the MACHINE user — resolve from its gh session or vault, never paste into the repo
sbx secret set github --command 'gh auth token'
```

On first `sbx run`, pick the **Balanced** network preset (default-deny with
common dev hosts allowed); use **Locked Down** when the task needs no extra
network. Inspect/extend with `sbx policy ls` / `sbx policy allow network <host>`.

## 3. Option 1: Docker AI Sandbox with `sbxenv.yaml` (preferred)

The repository-root `sbxenv.yaml` is the shared workspace definition for Docker
AI Sandbox. It selects the mise kit and declares non-secret environment
settings. Keep credentials in `sbx`/1Password secret stores; do not put token
values in `sbxenv.yaml`.

From the repository root, sign in once with `sbx login`, then run:

```bash
sbx run --name reporting-agent --clone opencode .  # or: claude | codex | gemini
```

`sbx run` with no workspace mounts the current directory read-write; `--clone`
keeps agent edits in an in-sandbox clone until you fetch or the agent pushes.
List / stop / remove with `sbx ls`, `sbx stop <name>`, `sbx rm <name>`.
See `https://docs.docker.com/ai/sandboxes/workflows/git/` for direct vs clone
vs host-worktree trade-offs.

Inside the sandbox, install the project toolchain and use the mise tasks:

```bash
mise install
mise run setup
opencode run "your task here"
# claude -p "..." | codex exec "..." — per agent installed in the sandbox
mise run test
mise run dbt-build-mock
mise run dbt-lint
```

Credentialed tasks call `op run --environment "$OP_ENVIRONMENT_ID" -- ...`.
Configure both `OP_ENVIRONMENT_ID` and `OP_SERVICE_ACCOUNT_TOKEN` in the
sandbox. The 1Password CLI must support Environments.
Databricks tasks start the configured SQL warehouse and wait until it is
healthy before running.

Git rules for agent work:

- Prefer clone mode + one branch per task; ask the agent to create the branch
  before editing.
- Review from the host before pushing to origin:
  `git fetch sandbox-<name>`, `git diff main..sandbox-<name>/feat/...`.
- Push/PR as the machine user (`git push`, `gh pr create`). Keep work on
  feature branches, never direct to `main`, never commit secrets
  (`.secrets/`, `*/secrets.toml`, `.env`).
- Default to `DBT_TARGET=mock` (DuckDB fixtures) inside sandboxes; touch
  Databricks only when the task requires it.

See `https://docs.docker.com/ai/sandboxes/configuration/environment-files/` for
the `sbxenv.yaml` format and lifecycle hooks. Use `sbx secret` to provision the
GitHub machine-user token, model credentials, `OP_SERVICE_ACCOUNT_TOKEN`, and
the 1Password Environment ID needed by mise tasks.

## 4. Option 2: agent devcontainer

Use the `agent` service in `.devcontainer/` when Docker AI Sandbox is not
available or when you specifically need the container workflow. It provides
the same mise task wiring with weaker isolation
(non-root `agent` user, no sudo, `read_only: true`, `cap_drop: [ALL]`,
`no-new-privileges`, ephemeral `tmpfs` for `/tmp` and `/home/agent`, no
published ports / Docker socket).

| Path | Role |
|---|---|
| `.devcontainer/docker-compose.yml` | Shared Compose file: `agent` service, `app` service (developer), `db` service (Postgres sidecar), Docker secrets |
| `.devcontainer/agent/devcontainer.json` | Devcontainer config **"AI Agents"** → `agent` service |
| `.devcontainer/developer/devcontainer.json` | Devcontainer config **"Reporting Developer"** → `app` service |
| `.devcontainer/Dockerfile.agent` | Agent image (least privilege) |
| `.devcontainer/Dockerfile.developer` | Developer image (full access) |
| `.devcontainer/scripts/entrypoint.sh` | Agent entrypoint (secrets, config copy, git identity) |

```bash
cd .devcontainer

# Build the agent image
docker compose build agent

# Start the sandbox (detached, sleeps forever)
GITHUB_NAME="YOUR-MACHINE-USER" GITHUB_EMAIL="machine-user@example.com" docker compose up -d agent

# Open a shell inside it (the checkout is at /workspaces/<repo-dir>)
docker compose exec agent bash
cd /workspaces/personal-reporting   # your clone directory name

# Tear down (home state is ephemeral and discarded)
docker compose down
```

Notes:

- Agent and GitHub credentials come from the host file
  `~/.secrets/.agent.env`. Compose mounts it as the Docker secret
  `/run/secrets/agent_env`. Use shell-compatible `KEY=value` entries for
  `OPENCODE_API_KEY`, `GITHUB_TOKEN` (the **machine-user PAT**),
  `OP_SERVICE_ACCOUNT_TOKEN`, and `OP_ENVIRONMENT_ID`. Also include
  `ANTHROPIC_API_KEY` and/or `OPENAI_API_KEY` if using those CLIs. The entrypoint
  sources this file with automatic export enabled for itself and adds a source
  command to the agent user's `.bashrc` for interactive shells. Do not copy
  values into the repository or image.
- VSCode alternative: "Reopen in Container" → **AI Agents**
  (`.devcontainer/agent/`) or **Reporting Developer**
  (`.devcontainer/developer/`); or
  `devcontainer up --workspace-folder . --config .devcontainer/agent/devcontainer.json`.
- Inside: `mise install`, `mise run setup`, then `opencode run "..."` /
  `claude -p "..."` / `codex exec "..."`. Credentialed tasks use the
  configured 1Password Environment.
  Commit as the machine user on feature branches; push over HTTPS with
  `GITHUB_TOKEN`.

## Preserve OpenCode session history

The agent home directory is an ephemeral `tmpfs`, so OpenCode sessions under
`~/.local/share/opencode/` disappear when the container or sandbox is removed.
Export sessions to durable storage before teardown and import them after
starting a replacement environment. See the OpenCode [CLI
documentation](https://opencode.ai/docs/cli/) for the `session list`, `export`,
and `import` commands.

These examples require `jq` and the Google Cloud CLI, authenticated with access
to the target bucket. They are not installed in the agent devcontainer image;
run them in an environment that has these tools and can access the OpenCode
session store. Install `jq` using its [installation instructions](https://jqlang.org/download/)
and the Google Cloud CLI using Google's [gcloud CLI installation guide](https://cloud.google.com/sdk/docs/install).
Replace the bucket name with one you can access:

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

To restore a session after starting the replacement environment, run from the
project directory where it should be available:

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

For the agent devcontainer, use the mounted workspace to move the export between
the container and host: get the session ID with `opencode session list` inside
the container, run `opencode export <session-id>` there with output directed to
`/workspaces/<repo>/<session-id>.json`, upload that file from the host with
`gcloud storage cp`, and remove it afterward. For restore, download the object
from the host into the mounted workspace, then run `opencode import` inside the
container. Never commit the export. Session JSON contains the transcript and may
include file contents or other workspace data; restrict the bucket's IAM access
and retention policy. See Google's [Cloud Storage upload](https://cloud.google.com/storage/docs/uploading-objects)
and [download](https://cloud.google.com/storage/docs/downloading-objects)
documentation for `gcloud storage cp`.

## Troubleshooting

- **`sbx` install / KVM errors**: follow
  `https://docs.docker.com/ai/sandboxes/install/` per OS (KVM group +
  re-login on Linux, Hypervisor Platform on Windows).
- **Agent cannot reach a host**: check `sbx policy ls`; allow it with
  `sbx policy allow network <host>`. Under Locked Down the model API is
  blocked until allowed.
- **GitHub auth as the wrong user**: confirm the `github` secret resolves the
  machine-user PAT (`gh auth status` on the host first); never fall back to a
  personal token with wider scopes.
- **Agent container — missing secrets**: confirm the host file
  `~/.secrets/.agent.env` exists and has shell-compatible `KEY=value` entries;
  Compose mounts it as `/run/secrets/agent_env`.
- **Agent container — lost home state after restart**: expected (`tmpfs`).
  The entrypoint restores secrets, configs, and git identity; project files in
  `/workspaces/<repo>` are unaffected.
