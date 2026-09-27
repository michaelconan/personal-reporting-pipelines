# Agentic Coding (Docker Sandbox + Fallback Devcontainer)

Primary path for AI agent development is **Docker AI Sandbox (`sbx` CLI)**
with a **dedicated GitHub machine user** scoped to this repo. The restricted
devcontainer under `.devcontainer/` is retained as a **fallback** when `sbx`
cannot run.

Why `sbx` is primary:

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

## 3. Run an agent (primary path)

From your repo checkout:

```bash
cd personal-reporting-pipelines

# Single-branch, turn-by-turn work: mounts the host tree directly
sbx run --name reporting-agent claude   # or: codex | opencode | gemini

# Agent-driven branches (recommended for PR work): private clone inside the sandbox
sbx run --name reporting-agent --clone claude .
```

`sbx run` with no workspace mounts the current directory read-write; `--clone`
keeps agent edits in an in-sandbox clone until you fetch or the agent pushes.
List / stop / remove with `sbx ls`, `sbx stop <name>`, `sbx rm <name>`.
See `https://docs.docker.com/ai/sandboxes/workflows/git/` for direct vs clone
vs host-worktree trade-offs.

Inside the sandbox, the project workflow is unchanged:

```bash
uv sync            # or: make install
make inject        # only when warehouse creds are needed (needs OP_SERVICE_ACCOUNT_TOKEN)
opencode run "your task here"
# claude -p "..." | codex exec "..." — per agent installed in the sandbox
make test-local
uv run dbt build --project-dir dbt --profiles-dir dbt --target mock
uv run dbt lint --project-dir dbt --profiles-dir dbt --target mock
```

Git rules for agent work:

- Prefer clone mode + one branch per task; ask the agent to create the branch
  before editing.
- Review from the host before pushing to origin:
  `git fetch sandbox-<name>`, `git diff main..sandbox-<name>/feat/...`.
- Push/PR as the machine user (`git push`, `gh pr create`). Keep work on
  feature branches, never direct to `main`, never commit secrets
  (`.secrets/`, `*/secrets.toml`, `.env.databricks`).
- Default to `DBT_TARGET=mock` (DuckDB fixtures) inside sandboxes; touch
  Databricks only when the task requires it.

Optional: capture the setup in an `sbxenv.yaml` kept **outside** the mounted
workspace so contributors share agent, secrets, and env without retyping flags
(see `https://docs.docker.com/ai/sandboxes/configuration/environment-files/`):

```yaml
schemaVersion: "1"
name: reporting-agent
agent: claude
workspace: ./personal-reporting-pipelines
env:
  DBT_TARGET: mock
secrets:
  github:
    command: gh auth token   # run as the machine user on the host
```

Then `sbx env run`, `sbx env exec -- <cmd>`, `sbx env rm` from the directory
holding the file.

## 4. Fallback: restricted devcontainer

Use the `agent` service in `.devcontainer/` only when `sbx` cannot run
(unsupported OS/virtualization, offline work, or debugging the container
itself). It provides the same project wiring with weaker isolation
(non-root `agent` user, no sudo, `read_only: true`, `cap_drop: [ALL]`,
`no-new-privileges`, ephemeral `tmpfs` for `/tmp` and `/home/agent`, no
published ports / Docker socket).

| Path | Role |
|---|---|
| `.devcontainer/docker-compose.yml` | Shared Compose file: `agent` service (fallback sandbox), `app` service (developer), `db` service (Postgres sidecar), Docker secrets |
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

- Secrets come from repo-root `.secrets/` (gitignored):
  `.secrets/opencode_api_key`, `.secrets/github_token` (**machine-user PAT**),
  `.secrets/op_service_account_token`. The entrypoint exports them to
  `OPENCODE_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY`, `GITHUB_TOKEN`,
  `OP_SERVICE_ACCOUNT_TOKEN`.
- VSCode alternative: "Reopen in Container" → **AI Agents**
  (`.devcontainer/agent/`) or **Reporting Developer**
  (`.devcontainer/developer/`); or
  `devcontainer up --workspace-folder . --config .devcontainer/agent/devcontainer.json`.
- Inside: `uv sync` (`make install`), `make inject` when warehouse creds are
  needed, then `opencode run "..."` / `claude -p "..."` / `codex exec "..."`.
  Commit as the machine user on feature branches; push over HTTPS with
  `GITHUB_TOKEN`.

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
- **Fallback container — missing secrets**: `docker compose` fails if a file
  under repo-root `.secrets/` is absent — create all three files first.
- **Fallback container — lost home state after restart**: expected (`tmpfs`).
  The entrypoint restores secrets, configs, and git identity; project files in
  `/workspaces/<repo>` are unaffected.
