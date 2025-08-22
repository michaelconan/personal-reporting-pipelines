FROM python:3.12-slim

# User and Group identifiers
ARG UID=1000
ARG GID=1000

# Add local bin to PATH
ENV PATH="/home/vscode/.local/bin:${PATH}"

# Install git and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git sudo curl make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install duckdb for local development
RUN curl https://install.duckdb.org | sh

# Create and run as non-root user
RUN addgroup --gid $GID vscode && \
    adduser --uid $UID --gid $GID vscode

# Allow the non-root user to run sudo without a password
RUN echo "vscode ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode

USER vscode