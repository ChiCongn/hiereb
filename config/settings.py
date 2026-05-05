"""
Central configuration for all HierEB services.
All values read from environment variables (or .env file).

Usage:
    from config.settings import settings
    print(settings.KAFKA_BOOTSTRAP_SERVERS)
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    # ── Kafka ─────────────────────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="kafka:29092")

    # ── Data ──────────────────────────────────────────────────────────────────
    DATA_PATH: str = Field(default="/data")
    DATA_FILE: str = Field(default="house-1.csv")
    # NOTE: pydantic-settings reads lists from env as JSON.
    # Example: HOUSE_IDS='[1]' or HOUSE_IDS='[0,1,2]'
    HOUSE_IDS: list[int] = Field(default=[1])
    PROPERTY_FILTER: int = Field(default=0)  # 0 = instantaneous load (Watts)

    # ── Simulator ─────────────────────────────────────────────────────────────
    REPLAY_SPEED: int = Field(default=60)          # 60x real-time
    SUPPRESSION_MODE: str = Field(default="full_tx")
    # full_tx  : always transmit (Week 1 baseline)
    # uniform  : suppress with fixed UNIFORM_DELTA
    # hiereb   : full HierEB water-filling (Week 2+)

    # ── HierEB Algorithm ──────────────────────────────────────────────────────
    EPSILON_H: float = Field(default=0.05)         # <1 = ratio of mean load; >=1 = Watts
    TAU: int = Field(default=300)                  # reallocation interval (seconds of data-time)
    UNIFORM_DELTA: float = Field(default=10.0)     # Watts, for 'uniform' mode
    RUN_ID: str = Field(default="default")         # experiment/run namespace for DB rows

    # ── TimescaleDB ───────────────────────────────────────────────────────────
    DB_HOST: str = Field(default="timescaledb")
    DB_PORT: int = Field(default=5432)
    DB_PORT_EXTERNAL: int = Field(default=5433)
    DB_USER: str = Field(default="hiereb")
    DB_PASSWORD: str = Field(default="hiereb_pass")
    DB_NAME: str = Field(default="hiereb_db")
    DB_POOL_SIZE: int = Field(default=5)
    DB_WRITE_BATCH_SIZE: int = Field(default=100)  # rows per INSERT batch

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = Field(default="INFO")

    # ── Timing ────────────────────────────────────────────────────────────────
    BATCH_INTERVAL_SECONDS: int = Field(default=300)
    FLUSH_DELAY_SECONDS: float = Field(default=0.1)

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
    }


# Module-level singleton – all services import this
settings = Settings()
