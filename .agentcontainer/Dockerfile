FROM python:3.13-slim

ARG UID=1000
ARG GID=1000
ARG OP_VERSION=2.32.0

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/home/agent/.local/bin:${PATH}"

# System packages: git, curl, unzip (op job), Node 22, OpenSSH client for SSH signing
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl unzip ca-certificates && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs openssh-client && \
    rm -rf /var/lib/apt/lists/*

# GitHub CLI (gh) - install from official apt repo
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends gh && \
    rm -rf /var/lib/apt/lists/* && \
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