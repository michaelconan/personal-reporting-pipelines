FROM python:3.13-slim

# User and Group identifiers
ARG UID=1000
ARG GID=1000

# Add local bin to PATH
ENV PATH="/home/vscode/.local/bin:${PATH}"

# Install git and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git sudo curl make gnupg2 && \
    # Install 1password CLI
    curl -sS https://downloads.1password.com/linux/keys/1password.asc | \
    sudo gpg --dearmor --output /usr/share/keyrings/1password-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/1password-archive-keyring.gpg] https://downloads.1password.com/linux/debian/$(dpkg --print-architecture) stable main" | \
    sudo tee /etc/apt/sources.list.d/1password.list && \
    sudo mkdir -p /etc/debsig/policies/AC2D62742012EA22/ && \
    curl -sS https://downloads.1password.com/linux/debian/debsig/1password.pol | \
    sudo tee /etc/debsig/policies/AC2D62742012EA22/1password.pol && \
    sudo mkdir -p /usr/share/debsig/keyrings/AC2D62742012EA22 && \
    curl -sS https://downloads.1password.com/linux/keys/1password.asc | \
    sudo gpg --dearmor --output /usr/share/debsig/keyrings/AC2D62742012EA22/debsig.gpg && \
    sudo apt update && sudo apt install 1password-cli && \
    # Cleanup apt cache
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install duckdb for local development
RUN curl https://install.duckdb.org | sh

# Install pipenv for dependency management
RUN pip install --upgrade pip && pip install pipenv

# Create non-root user (GID 1000 is already in use, so use 1001)
RUN groupadd --gid 1001 vscode && \
    useradd --uid 1000 --gid 1001 --shell /bin/bash --create-home vscode

# Allow the non-root user to run sudo without a password
RUN echo "vscode ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode

USER vscode