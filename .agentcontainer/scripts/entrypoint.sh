#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/home/agent}"

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

git config --global user.name "${GITHUB_NAME}"
git config --global user.email "${GITHUB_EMAIL}"

# SSH agent is forwarded from the host (docker-compose binds
# /run/host-services/ssh-auth.sock and sets SSH_AUTH_SOCK). No keys are
# copied into the container.
if [[ -n "${SSH_AUTH_SOCK:-}" ]] && ssh-add -l >/dev/null 2>&1; then
    FIRST_KEY="$(ssh-add -L | head -n1 | awk '{print $1, $2}')"
    if [[ -n "${FIRST_KEY}" ]]; then
        git config --global gpg.format ssh
        git config --global user.signingkey "key::${FIRST_KEY}"
    fi
fi

exec "$@"