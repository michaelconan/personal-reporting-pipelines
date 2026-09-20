FROM python:3.13-slim

ARG UID=1000
ARG GID=1000
ARG OP_VERSION=2.32.0
# Service-account token for the op CLI, injected at build time from the local
# 1Password CLI (e.g. `OP_SERVICE_ACCOUNT_TOKEN="$(op read op://...)" docker compose build`).
ARG OP_SERVICE_ACCOUNT_TOKEN

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/home/agent/.local/bin:${PATH}" \
    OP_SERVICE_ACCOUNT_TOKEN="${OP_SERVICE_ACCOUNT_TOKEN}"

# System packages: git, curl, unzip (op job), Node 22, OpenSSH client for SSH signing
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl unzip ca-certificates && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs openssh-client && \
    rm -rf /var/lib/apt/lists/*

# GitHub CLI (gh)
RUN ARCH="$(dpkg --print-architecture)" && \
    curl -fsSL "https://github.com/cli/cli/releases/latest/download/gh_2.67.0_linux_${ARCH}.tar.gz" -o /tmp/gh.tar.gz && \
    tar -xzf /tmp/gh.tar.gz -C /tmp && \
    install -m 0755 /tmp/gh_*/bin/gh /usr/local/bin/gh && \
    rm -rf /tmp/gh /tmp/gh.tar.gz && \
    gh --version

# 1Password CLI via direct download (avoids the gpg/debsig apt-repo ceremony)
RUN ARCH="$(dpkg --print-architecture)" && \
    curl -fsSL "https://cache.agilebits.com/dist/1P/op2/pkg/v${OP_VERSION}/op_linux_${ARCH}_v${OP_VERSION}.zip" -o /tmp/op.zip && \
    unzip -q /tmp/op.zip -d /tmp/op && \
    install -m 0755 /tmp/op/op /usr/local/bin/op && \
    rm -rf /tmp/op /tmp/op.zip && \
    op --version

# AI agent CLIs (global installs land in /usr/local, read-only-safe at runtime)
RUN npm install -g opencode-ai @anthropic-ai/claude-code @openai/codex && \
    npm cache clean --force

# Project tooling: uv (dlt/dbt workflows) + python common deps
RUN pip install --no-cache-dir uv

# Non-root user with NO sudo (limited access)
RUN (groupadd --gid "$GID" agent 2>/dev/null || groupadd agent) && \
    useradd --uid "$UID" --gid agent --shell /bin/bash --create-home agent

# Entrypoint prepares the agent home and configures SSH signing from the forwarded agent
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER agent
WORKDIR /workspaces
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/bin/bash"]