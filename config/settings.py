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
    # DATASET_PRESET chooses the source files without changing code.
    # Hyphen aliases are accepted: one-house/five-house/all-house.
    # Canonical values: custom | one_house | five_houses | all_house
    DATASET_PRESET: str = Field(default="custom")
    # DATA_WINDOW controls experiment duration/source subset:
    # one_day/1d | five_days/5d | a_week/7d | all
    DATA_WINDOW: str = Field(default="all")
    DATA_FILE: str = Field(default="house-1.csv")
    DATA_GLOB: str = Field(default="")
    ONE_HOUSE_ID: int = Field(default=1)
    FIVE_HOUSE_IDS: list[int] = Field(default_factory=lambda: [0, 1, 2, 3, 4])
    # NOTE: pydantic-settings reads lists from env as JSON.
    # Example: HOUSE_IDS='[1]' or HOUSE_IDS='[0,1,2]'
    HOUSE_IDS: list[int] = Field(default=[1])
    PROPERTY_FILTER: int = Field(default=0)  # 0 = instantaneous load (Watts)

    # ── Simulator ─────────────────────────────────────────────────────────────
    REPLAY_SPEED: int = Field(default=60)          # 60x real-time
    SUPPRESSION_MODE: str = Field(default="full_tx")
    # wall_clock: write simulated stream timestamps near current time.
    # source: keep original DEBS unix timestamps.
    STREAM_TIME_MODE: str = Field(default="wall_clock")
    # full_tx  : always transmit (Week 1 baseline)
    # uniform  : suppress with fixed UNIFORM_DELTA
    # hiereb   : full HierEB water-filling (Week 2+)

    # ── HierEB Algorithm ──────────────────────────────────────────────────────
    EPSILON_H: float = Field(default=0.05)         # <1 = ratio of mean load; >=1 = Watts
    TAU: int = Field(default=300)                  # reallocation interval (seconds of data-time)
    UNIFORM_DELTA: float = Field(default=10.0)     # Watts, for 'uniform' mode
    RUN_ID: str = Field(default="default")         # experiment/run namespace for DB rows
    TRAINING_DAYS: int = Field(default=7)          # <=0 means use all loaded data
    # Prediction granularity for time-slice median. 300 = 5-minute bins.
    PREDICTOR_BIN_SECONDS: int = Field(default=300)
    # Simulator keeps prediction batches around the current source timestamp.
    PREDICTION_CACHE_MAX_SECONDS: int = Field(default=1800)

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
