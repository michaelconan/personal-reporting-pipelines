#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/home/agent}"

# ---------------------------------------------------------------------------
# 1. Load Docker secrets into environment variables
# ---------------------------------------------------------------------------
# In docker-compose, secrets are mounted as files under /run/secrets/.
# The AI agent CLIs expect env vars, so we read and export them here.
# Falls back gracefully if running outside compose (e.g. devcontainer).
# Persist secrets to a file sourced by interactive shells
SECRETS_ENV_FILE="$HOME/.secrets_env"
: > "$SECRETS_ENV_FILE"

if [[ -d /run/secrets ]]; then
    for secret_file in /run/secrets/*; do
        [[ -f "$secret_file" ]] || continue
        secret_name="$(basename "$secret_file")"
        secret_value="$(cat "$secret_file")"

        case "$secret_name" in
            opencode_api_key)
                export OPENCODE_API_KEY="$secret_value"
                export ANTHROPIC_API_KEY="$secret_value"
                export OPENAI_API_KEY="$secret_value"
                echo "export OPENCODE_API_KEY=\"$secret_value\"" >> "$SECRETS_ENV_FILE"
                echo "export ANTHROPIC_API_KEY=\"$secret_value\"" >> "$SECRETS_ENV_FILE"
                echo "export OPENAI_API_KEY=\"$secret_value\"" >> "$SECRETS_ENV_FILE"
                ;;
            github_token)
                export GITHUB_TOKEN="$secret_value"
                echo "export GITHUB_TOKEN=\"$secret_value\"" >> "$SECRETS_ENV_FILE"
                ;;
            op_service_account_token)
                export OP_SERVICE_ACCOUNT_TOKEN="$secret_value"
                echo "export OP_SERVICE_ACCOUNT_TOKEN=\"$secret_value\"" >> "$SECRETS_ENV_FILE"
                ;;
        esac
    done
fi

# Source the persisted secrets for the current shell
[[ -f "$SECRETS_ENV_FILE" ]] && source "$SECRETS_ENV_FILE"

# Ensure future interactive shells load the secrets
if [[ -f "$HOME/.bashrc" ]]; then
    grep -q "source $SECRETS_ENV_FILE" "$HOME/.bashrc" || echo "source $SECRETS_ENV_FILE" >> "$HOME/.bashrc"
else
    echo "source $SECRETS_ENV_FILE" > "$HOME/.bashrc"
fi

# ---------------------------------------------------------------------------
# 2. Copy user-level agent config files from host mounts
# ---------------------------------------------------------------------------
# Claude and Codex use their home directories for both config AND writable
# session/state data. We mount host configs to staging paths and copy only
# the config files, leaving agents free to write state to their home dirs.

# --- Claude Code ---
HOST_CLAUDE="/home/agent/.host-claude"
if [[ -d "$HOST_CLAUDE" ]]; then
    mkdir -p "$HOME/.claude"

    # Config files
    for f in settings.json CLAUDE.md keybindings.json; do
        [[ -f "$HOST_CLAUDE/$f" ]] && cp -p "$HOST_CLAUDE/$f" "$HOME/.claude/$f"
    done

    # Config directories
    for d in themes rules skills agents workflows output-styles; do
        [[ -d "$HOST_CLAUDE/$d" ]] && cp -rp "$HOST_CLAUDE/$d" "$HOME/.claude/$d"
    done
fi

# --- Codex CLI ---
HOST_CODEX="/home/agent/.host-codex"
if [[ -d "$HOST_CODEX" ]]; then
    mkdir -p "$HOME/.codex"

    # Config files
    for f in config.toml hooks.json AGENTS.md; do
        [[ -f "$HOST_CODEX/$f" ]] && cp -p "$HOST_CODEX/$f" "$HOME/.codex/$f"
    done

    # Named profile configs (*.config.toml)
    for f in "$HOST_CODEX"/*.config.toml; do
        [[ -f "$f" ]] && cp -p "$f" "$HOME/.codex/$(basename "$f")"
    done
fi

# --- OpenCode ---
# Config is already in ~/.config/opencode/ via direct read-only mount.
# No copy needed — state lives separately at ~/.local/share/opencode/.

# --- dlthub skills ---
# Already mounted directly at ~/.agents/ (read-only). No copy needed.

# ---------------------------------------------------------------------------
# 3. SSH agent forwarding & git config
# ---------------------------------------------------------------------------
mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

git config --global user.name "${GITHUB_NAME}"
git config --global user.email "${GITHUB_EMAIL}"

# SSH agent is forwarded from the host (docker-compose binds
# /run/host-services/ssh-auth.sock and sets SSH_AUTH_SOCK). No keys are
# copied into the container.
if [[ -z "${SSH_AUTH_SOCK:-}" ]]; then
    echo "WARNING: SSH agent forwarding is not configured; SSH commit signing is disabled." >&2
    echo "         Use GITHUB_TOKEN for HTTPS Git operations; see docs/source/agentcontainer.md." >&2
elif [[ ! -S "$SSH_AUTH_SOCK" ]]; then
    echo "WARNING: SSH agent socket is missing: $SSH_AUTH_SOCK" >&2
    echo "         SSH commit signing is disabled. Restart Docker Desktop/Dev Containers after" >&2
    echo "         configuring host forwarding, or use the GITHUB_TOKEN HTTPS fallback." >&2
elif ssh-add -l >/dev/null 2>&1; then
    FIRST_KEY="$(ssh-add -L | head -n1 | awk '{print $1, $2}')"
    if [[ -n "${FIRST_KEY}" ]]; then
        git config --global gpg.format ssh
        git config --global user.signingkey "key::${FIRST_KEY}"
    else
        echo "WARNING: SSH agent returned no public key; SSH commit signing is disabled." >&2
    fi
else
    SSH_ADD_STATUS=$?
    if [[ "$SSH_ADD_STATUS" -eq 1 ]]; then
        echo "WARNING: SSH agent socket is present but has no identities: $SSH_AUTH_SOCK" >&2
        echo "         Load a key in the host agent and rebuild the container, or use the" >&2
        echo "         GITHUB_TOKEN HTTPS fallback; see docs/source/agentcontainer.md." >&2
    else
        echo "WARNING: SSH agent could not be queried: $SSH_AUTH_SOCK" >&2
        echo "         SSH commit signing is disabled; use the GITHUB_TOKEN HTTPS fallback." >&2
    fi
fi

exec "$@"
