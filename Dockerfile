# ─────────────────────────────────────────────────────────────────────────────
# HierEB – Production Dockerfile
# Shared by: simulator, aggregator, ml-hiereb
# CMD is set per-service in docker-compose.yml via `command:`
#
# Build:
#   docker build -t hiereb:latest .
#
# Run (example – normally done via docker compose):
#   docker run --rm --env-file .env hiereb:latest python -m src.simulator.main
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: dependency builder ───────────────────────────────────────────────
# Separate stage so the final image doesn't carry gcc / libpq-dev (~120MB saved)
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed to compile C extensions:
#   gcc       → builds asyncpg (C extension)
#   libpq-dev → asyncpg links against libpq at compile time
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifest first → layer-cache: pip install skipped if unchanged
COPY pyproject.toml .

# Install runtime dependencies into an isolated prefix /install
RUN pip install --no-cache-dir --prefix=/install \
    "aiokafka>=0.10.0" \
    "asyncpg>=0.29.0" \
    "pandas>=2.0.0" \
    "numpy>=1.26.0" \
    "pydantic>=2.0.0" \
    "pydantic-settings>=2.0.0" \
    "structlog>=24.0.0"

# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime-only system lib: asyncpg links against libpq at runtime.
# libpq5 is ~1MB vs libpq-dev ~10MB.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled packages from builder stage only
COPY --from=builder /install /usr/local

# Copy application source (least-changed → most-changed for layer cache)
COPY pyproject.toml .
COPY config/ config/
COPY src/     src/

# Register the package so `python -m src.*.main` resolves correctly.
# --no-deps: all deps already installed above, skip resolution.
RUN pip install --no-cache-dir --no-deps -e .

# Non-root user: limits blast radius even in research environments
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app

USER appuser

# Runtime environment
ENV LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# No ENTRYPOINT – CMD overridden per-service in docker-compose.yml
CMD ["python", "-c", "print('Specify command in docker-compose.yml')"]