#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/home/agent}"

# ---------------------------------------------------------------------------
# 1. Load the combined agent environment file
# ---------------------------------------------------------------------------
# Docker Compose mounts the host agent .env file as /run/secrets/agent_env.
# Export every assignment for this process and its child processes without
# copying the secret values into the container's writable filesystem.
AGENT_ENV_FILE="/run/secrets/agent_env"
if [[ -f "$AGENT_ENV_FILE" ]]; then
    set -a
    # The mounted file is a trusted, shell-compatible KEY=value environment file.
    # shellcheck disable=SC1090
    source "$AGENT_ENV_FILE"
    set +a

    # VS Code starts interactive shells separately from the entrypoint process.
    # Source the mounted file there as well so those shells inherit the variables.
    AGENT_ENV_SOURCE="set -a; source $AGENT_ENV_FILE; set +a"
    if [[ -f "$HOME/.bashrc" ]]; then
        grep -Fq "$AGENT_ENV_SOURCE" "$HOME/.bashrc" || \
            printf '\n%s\n' "$AGENT_ENV_SOURCE" >> "$HOME/.bashrc"
    else
        printf '%s\n' "$AGENT_ENV_SOURCE" > "$HOME/.bashrc"
    fi
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

# SSH agent forwarding is optional (requires Docker Desktop host services).
# If SSH_AUTH_SOCK is set and the socket is accessible, configure git signing.
if [[ -n "${SSH_AUTH_SOCK:-}" ]] && [[ -S "${SSH_AUTH_SOCK}" ]] && ssh-add -l >/dev/null 2>&1; then
    FIRST_KEY="$(ssh-add -L | head -n1 | awk '{print $1, $2}')"
    if [[ -n "${FIRST_KEY}" ]]; then
        git config --global gpg.format ssh
        git config --global user.signingkey "key::${FIRST_KEY}"
    fi
fi

exec "$@"
