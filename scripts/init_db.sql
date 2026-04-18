-- HierEB – TimescaleDB Schema
-- Runs automatically on first container start (mounted into docker-entrypoint-initdb.d/)

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─────────────────────────────────────────────────────────────────────────────
-- Main metrics table
-- One row per (house, timestep). Written by the Aggregator.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS house_metrics (
    time              TIMESTAMPTZ  NOT NULL,
    house_id          SMALLINT     NOT NULL,
    actual_load       REAL,          -- total house load (W); suppressed plugs use predicted
    pred_load         REAL,          -- sum of predicted values (W)
    e_h               REAL,          -- house error: actual_load - pred_load
    tr                REAL,          -- transmission rate [0, 1]
    plug_count        SMALLINT,      -- total plugs seen this timestep
    transmitted_count SMALLINT       -- plugs that actually transmitted
);

SELECT create_hypertable('house_metrics', 'time', if_not_exists => TRUE);
SELECT set_chunk_time_interval('house_metrics', INTERVAL '1 hour');

CREATE INDEX IF NOT EXISTS idx_house_metrics_house_time
    ON house_metrics (house_id, time DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- Threshold history
-- One row per (plug, reallocation cycle). Written by ML/HierEB service (Week 2+).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS threshold_log (
    time         TIMESTAMPTZ  NOT NULL,
    plug_uid     INTEGER      NOT NULL,
    house_id     SMALLINT     NOT NULL,
    household_id SMALLINT     NOT NULL,
    plug_id      SMALLINT     NOT NULL,
    delta_p      REAL,          -- current threshold (W)
    sigma_p      REAL,          -- estimated std dev of prediction error (W)
    tr_plug      REAL           -- TR of this plug since last reallocation
);

SELECT create_hypertable('threshold_log', 'time', if_not_exists => TRUE);
SELECT set_chunk_time_interval('threshold_log', INTERVAL '6 hours');

CREATE INDEX IF NOT EXISTS idx_threshold_log_plug_time
    ON threshold_log (plug_uid, time DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- Experiment runs
-- One row per experiment invocation (for paper comparison tables).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS experiment_runs (
    run_id     SERIAL       PRIMARY KEY,
    started_at TIMESTAMPTZ  DEFAULT NOW(),
    mode       TEXT         NOT NULL,     -- 'full_tx' | 'uniform' | 'hiereb'
    epsilon_h  REAL,
    tau        INTEGER,
    house_ids  INTEGER[],
    notes      TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Aggregated results per run
-- Written by export_results.py after a run completes.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS run_results (
    run_id         INTEGER REFERENCES experiment_runs(run_id),
    house_id       SMALLINT,
    avg_tr         REAL,
    rmse           REAL,
    violation_rate REAL,     -- P(|e_h| > epsilon_h)
    p90_e_h        REAL      -- 90th percentile of |e_h|
);