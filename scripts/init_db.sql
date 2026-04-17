CREATE TABLE house_metrics (
    time        TIMESTAMPTZ NOT NULL,
    house_id    SMALLINT NOT NULL,
    actual_load REAL,
    pred_load   REAL,
    e_h         REAL,      -- house-level error
    rmse_1min   REAL,      -- rolling RMSE (tính trong app)
    tr          REAL       -- transmission rate tại timestep này
);
SELECT create_hypertable('house_metrics', 'time');
SELECT set_chunk_time_interval('house_metrics', INTERVAL '1 hour');
CREATE INDEX ON house_metrics (house_id, time DESC);

-- Threshold history (cho ablation study và Figure δ_p evolution)
CREATE TABLE threshold_log (
    time     TIMESTAMPTZ NOT NULL,
    plug_id  INTEGER NOT NULL,
    house_id SMALLINT NOT NULL,
    delta_p  REAL,
    sigma_p  REAL,
    tr_plug  REAL    -- TR của riêng plug này
);
SELECT create_hypertable('threshold_log', 'time');
SELECT set_chunk_time_interval('threshold_log', INTERVAL '6 hours');

-- Experiment runs (để so sánh baseline vs HierEB)
CREATE TABLE experiment_runs (
    run_id      SERIAL PRIMARY KEY,
    started_at  TIMESTAMPTZ DEFAULT NOW(),
    mode        TEXT,        -- 'full_tx', 'uniform', 'hiereb'
    epsilon_h   REAL,
    tau         INTEGER,
    house_ids   INTEGER[],
    notes       TEXT
);

-- Aggregate results per run (cho paper tables)
CREATE TABLE run_results (
    run_id      INTEGER REFERENCES experiment_runs(run_id),
    house_id    SMALLINT,
    avg_tr      REAL,
    rmse        REAL,
    violation_rate REAL,    -- P(|E_h| > epsilon_h)
    p90_e_h     REAL        -- 90th percentile của |E_h|
);