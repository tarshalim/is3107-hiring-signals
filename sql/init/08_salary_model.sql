-- Salary estimator (the proposal's stretch goal): where transform/salary_model.py stores its trained model.
--
-- A separate schema from `mart` because the marts are dropped and rebuilt every day, and the model must
-- survive that. The model is a ridge regression on log(monthly salary midpoint), so it is just an
-- intercept and one coefficient per feature: stored as plain rows, no pickled objects, and the dashboard
-- predicts with a sum. IF NOT EXISTS everywhere: safe to run on every start and from the trainer.

CREATE SCHEMA IF NOT EXISTS ml;

-- One row per training run, with the held-out test metrics the proposal asks for (mean absolute error
-- within 15% of the advertised midpoint).
CREATE TABLE IF NOT EXISTS ml.salary_model_run (
    run_id         SERIAL PRIMARY KEY,
    trained_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    snapshot_date  DATE        NOT NULL,           -- latest snapshot day when the model was trained
    n_train        INTEGER     NOT NULL,
    n_test         INTEGER     NOT NULL,
    n_features     INTEGER     NOT NULL,
    alpha          DOUBLE PRECISION NOT NULL,      -- ridge penalty
    intercept      DOUBLE PRECISION NOT NULL,      -- in log SGD
    mae            DOUBLE PRECISION NOT NULL,      -- mean absolute error, SGD per month, on the test set
    mape           DOUBLE PRECISION NOT NULL,      -- mean |pred - actual| / actual, on the test set
    median_ape     DOUBLE PRECISION NOT NULL,
    within_15      DOUBLE PRECISION NOT NULL,      -- share of test postings predicted within 15% of actual
    baseline_mape  DOUBLE PRECISION NOT NULL,      -- same metric for "median salary of the occupation group"
    resid_p10      DOUBLE PRECISION NOT NULL,      -- 10th / 90th percentile of ln(actual / predicted):
    resid_p90      DOUBLE PRECISION NOT NULL       -- the dashboard's "likely range"
);

-- One row per feature: kind is skill, ssoc1, ssoc3, level, category or numeric.
CREATE TABLE IF NOT EXISTS ml.salary_model_coef (
    run_id    INTEGER NOT NULL REFERENCES ml.salary_model_run (run_id) ON DELETE CASCADE,
    feature   TEXT    NOT NULL,                    -- e.g. 'skill:ssg-123', 'level:Manager', 'years_exp'
    kind      TEXT    NOT NULL,
    label     TEXT    NOT NULL,
    coef      DOUBLE PRECISION NOT NULL,
    support   INTEGER NOT NULL DEFAULT 0,          -- training postings that have this feature
    PRIMARY KEY (run_id, feature)
);
