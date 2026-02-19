
# Stage 1: Builder
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
# --frozen: sync from uv.lock
# --no-install-project: only install dependencies, not the project itself yet
RUN uv sync --frozen --no-install-project

# Stage 2: Runner
FROM python:3.11-slim-bookworm

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Set environment variables to use the virtual environment
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy source code and config
COPY src/ src/
COPY sensor_config.yaml .
COPY serviceAccountKey.json . 

# Run the application
CMD ["python", "src/main.py"]
