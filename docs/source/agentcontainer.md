# Agent Container Authentication

The agent container does not copy private keys. Docker Compose asks Docker
Desktop to expose the host agent at `/run/host-services/ssh-auth.sock`, and the
entrypoint uses that agent for SSH commit signing and SSH-based Git operations.

## Verify forwarding

Run these commands inside the container:

```bash
if [[ ! -S "${SSH_AUTH_SOCK:-}" ]]; then
    printf 'SSH agent socket is missing: %s\n' "${SSH_AUTH_SOCK:-<unset>}" >&2
    exit 1
fi

if ! ssh-add -l; then
    printf 'SSH agent has no usable identities\n' >&2
    exit 1
fi
```

The entrypoint prints the same diagnosis during startup but deliberately does
not abort the container. This allows the HTTPS fallback to remain usable.

## Windows and WSL2

Docker Desktop's WSL2 integration and the Windows OpenSSH agent are separate
layers. Confirm that Docker Desktop is using Linux containers, WSL integration
is enabled for the distribution that owns the checkout, and the host agent
contains an identity before reopening the container. The VS Code guidance for
[sharing Git credentials with a container](https://code.visualstudio.com/remote/advancedcontainers/sharing-git-credentials)
covers enabling the Windows `ssh-agent` service and checking `ssh-add -l`.
Docker's [WSL 2 integration documentation](https://docs.docker.com/desktop/features/wsl/)
covers the required Docker Desktop integration settings.

There is no reliable in-container fix for a Windows named pipe that is not
being exposed as `/run/host-services/ssh-auth.sock`. If the host uses the
1Password SSH agent, follow [1Password's SSH agent documentation](https://developer.1password.com/docs/ssh/agent/)
and ensure the agent is available to the Windows/WSL layer used to start the
container. After changing host-side forwarding, rebuild or reopen the
container and rerun the verification commands above.

## HTTPS fallback

Compose mounts the `github_token` Docker secret and the entrypoint exports it
as `GITHUB_TOKEN`. Use GitHub CLI to configure Git without putting the token in
a remote URL:

```bash
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    printf 'GITHUB_TOKEN is not available\n' >&2
    exit 1
fi

if ! printf '%s\n' "$GITHUB_TOKEN" | gh auth login --with-token; then
    printf 'GitHub CLI authentication failed\n' >&2
    exit 1
fi

if ! gh auth setup-git; then
    printf 'Could not configure GitHub CLI as Git credential helper\n' >&2
    exit 1
fi
```

Use an HTTPS remote (`https://github.com/OWNER/REPOSITORY.git`) for Git
operations. GitHub documents the distinction between [SSH and HTTPS remote
URLs](https://docs.github.com/en/get-started/git-basics/about-remote-repositories#cloning-with-https-urls).
This fallback authenticates Git operations but does not provide SSH commit
signatures; configure another signing method if signed commits are required.
