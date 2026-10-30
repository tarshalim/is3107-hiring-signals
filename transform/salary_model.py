"""Salary estimator (the proposal's stretch goal): predicts the monthly salary midpoint of a posting from its
skills, occupation, position level, category and years of experience asked.

    python -m transform.salary_model          # train on the warehouse and store the model in schema `ml`

Model: ridge regression on log(salary). It is a sum of coefficients, so
  * it needs nothing beyond numpy (no scikit-learn, no pickled objects: the coefficients are rows in
    ml.salary_model_coef and the dashboard predicts with row_features() and a sum),
  * every coefficient reads as "this feature is associated with about +x% pay" (exp(coef) - 1), which is
    what a job seeker deciding what to upskill in wants to see.
A gradient-boosted tree would capture interactions (a skill that pays more in one occupation than another)
but is not explainable this way, so it is the next step only if the error target is missed.

Evaluation follows the proposal: a held-out test set (a fixed 20% of postings, chosen by a hash of the
posting id so it never changes between runs) and the error as a share of the advertised midpoint. The
baseline is "the median salary of the posting's occupation group", to show what the skills add.
These coefficients are associations in advertised salaries, not proof that learning a skill raises pay.
"""
import math
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

MODEL_SQL = Path(__file__).resolve().parent.parent / "sql" / "init" / "04_salary_model.sql"

ALPHA = 3.0                 # ridge penalty (features are 0/1, so this is "3 postings' worth" of shrinkage)
MIN_SUPPORT = 20            # a feature needs this many training postings, otherwise it is noise
MAX_PER_KIND = {"skill": 400, "ssoc3": 300, "level": 20, "category": 40}
MIN_TRAIN_ROWS = 300
MIN_TEST_ROWS = 50
TEST_PERCENT = 20
TARGET_ERROR = 0.15         # proposal: error within 15% of the advertised midpoint
CHUNK = 20_000
YEARS_SCALE = 10.0
MAX_YEARS = 40


def ssoc_parts(code) -> tuple:
    """(major group, 3-digit minor group) of a 5-digit SSOC code; '?' for a missing or non-numeric one."""
    code = (code or "").strip()
    if len(code) >= 3 and code[:3].isdigit():
        return code[:1], code[:3]
    return "?", "?"


def row_features(row: dict) -> list:
    """(feature name, value) pairs for one posting. Used by the trainer and by the dashboard, so both
    always agree on what a feature is. Keys: ssoc_code, position_level, category, min_years_exp, skills."""
    major, minor = ssoc_parts(row.get("ssoc_code"))
    feats = [(f"ssoc1:{major}", 1.0), (f"ssoc3:{minor}", 1.0)]
    if row.get("position_level"):
        feats.append((f"level:{row['position_level']}", 1.0))
    if row.get("category"):
        feats.append((f"category:{row['category']}", 1.0))
    years = row.get("min_years_exp")
    feats.append(("years_exp", min(max(years or 0, 0), MAX_YEARS) / YEARS_SCALE))
    feats.extend((f"skill:{s}", 1.0) for s in sorted(set(row.get("skills") or ())))
    return feats


def feature_kind(feature: str) -> str:
    return feature.split(":", 1)[0] if ":" in feature else "numeric"


@dataclass
class SalaryModel:
    intercept: float
    coefs: dict                                     # feature -> coefficient (log SGD)
    labels: dict = field(default_factory=dict)      # feature -> text shown in the dashboard
    support: dict = field(default_factory=dict)     # feature -> training postings with it
    resid_p10: float = -0.2
    resid_p90: float = 0.2
    metrics: dict = field(default_factory=dict)

    def predict(self, row: dict) -> dict:
        """Estimated monthly salary midpoint, a likely range, and what each part of the posting contributes."""
        log_salary = self.intercept
        parts = []
        for name, value in row_features(row):
            coef = self.coefs.get(name)
            if coef is None:
                continue
            log_salary += coef * value
            parts.append({"feature": name, "kind": feature_kind(name), "label": self.labels.get(name, name),
                          "effect_pct": (math.exp(coef * value) - 1) * 100})
        return {"estimate": math.exp(log_salary),
                "low": math.exp(log_salary + self.resid_p10),
                "high": math.exp(log_salary + self.resid_p90),
                "parts": parts}


def _vocabulary(rows) -> list:
    """Features kept for training: enough support, and at most MAX_PER_KIND per kind (most common first)."""
    counts = Counter()
    for row in rows:
        counts.update(name for name, _ in row_features(row))
    kept = ["years_exp"]
    for kind, limit in MAX_PER_KIND.items():
        common = [(n, c) for n, c in counts.items() if feature_kind(n) == kind and c >= MIN_SUPPORT]
        common.sort(key=lambda nc: (-nc[1], nc[0]))
        kept.extend(n for n, _ in common[:limit])
    kept.extend(sorted(n for n in counts if feature_kind(n) == "ssoc1" and counts[n] >= MIN_SUPPORT))
    return kept, counts


def _design(rows, index, n_cols):
    X = np.zeros((len(rows), n_cols), dtype=np.float32)
    X[:, 0] = 1.0                                   # intercept
    for i, row in enumerate(rows):
        for name, value in row_features(row):
            j = index.get(name)
            if j is not None:
                X[i, j] = value
    return X


def _chunks(rows, index, n_cols):
    for start in range(0, len(rows), CHUNK):
        part = rows[start:start + CHUNK]
        yield start, _design(part, index, n_cols).astype(np.float64)


def fit(train_rows: list, test_rows: list, alpha: float = ALPHA) -> SalaryModel:
    """Ridge regression of log(salary) on the features, then the held-out metrics. Rows also need 'salary'."""
    vocabulary, counts = _vocabulary(train_rows)
    index = {name: i + 1 for i, name in enumerate(vocabulary)}       # column 0 is the intercept
    n_cols = len(vocabulary) + 1

    xtx = np.zeros((n_cols, n_cols))
    xty = np.zeros(n_cols)
    log_salary = np.log(np.array([r["salary"] for r in train_rows], dtype=np.float64))
    for start, X in _chunks(train_rows, index, n_cols):
        xtx += X.T @ X
        xty += X.T @ log_salary[start:start + len(X)]
    penalty = np.eye(n_cols) * alpha
    penalty[0, 0] = 0.0                                              # the intercept is not shrunk
    weights = np.linalg.solve(xtx + penalty, xty)

    model = SalaryModel(
        intercept=float(weights[0]),
        coefs={name: float(weights[i]) for name, i in index.items()},
        labels={name: name.split(":", 1)[-1] if ":" in name else "Years of experience (per 10 years)"
                for name in vocabulary},
        support={name: int(counts[name]) for name in vocabulary if name in counts},
    )

    actual = np.array([r["salary"] for r in test_rows], dtype=np.float64)
    predicted = np.concatenate([np.exp(X @ weights) for _, X in _chunks(test_rows, index, n_cols)])
    abs_pct = np.abs(predicted - actual) / actual
    log_resid = np.log(actual / predicted)
    baseline = _baseline_predictions(train_rows, test_rows)
    model.resid_p10, model.resid_p90 = (float(q) for q in np.percentile(log_resid, [10, 90]))
    model.metrics = {
        "n_train": len(train_rows), "n_test": len(test_rows), "n_features": len(vocabulary),
        "alpha": alpha,
        "mae": float(np.mean(np.abs(predicted - actual))),
        "mape": float(np.mean(abs_pct)),
        "median_ape": float(np.median(abs_pct)),
        "within_15": float(np.mean(abs_pct <= TARGET_ERROR)),
        "baseline_mape": float(np.mean(np.abs(baseline - actual) / actual)),
    }
    return model


def _baseline_predictions(train_rows, test_rows) -> np.ndarray:
    """Median training salary of the posting's 3-digit occupation group, else its major group, else overall."""
    by_minor, by_major = {}, {}
    for r in train_rows:
        major, minor = ssoc_parts(r.get("ssoc_code"))
        by_minor.setdefault(minor, []).append(r["salary"])
        by_major.setdefault(major, []).append(r["salary"])
    overall = float(np.median([r["salary"] for r in train_rows]))
    minor_med = {k: float(np.median(v)) for k, v in by_minor.items() if len(v) >= 5}
    major_med = {k: float(np.median(v)) for k, v in by_major.items()}
    out = []
    for r in test_rows:
        major, minor = ssoc_parts(r.get("ssoc_code"))
        out.append(minor_med.get(minor, major_med.get(major, overall)))
    return np.array(out, dtype=np.float64)


# --- database -----------------------------------------------------------------------------------

# is_test comes from the md5 of the posting id, so the split is the same on every run and every machine.
TRAINING_SQL = """
SELECT j.ssoc_code, j.position_level, j.category, j.min_years_exp, j.salary_mid::float8 AS salary,
       (('x' || substr(md5(j.job_uuid), 1, 8))::bit(32)::bigint %% 100) < %(test)s AS is_test,
       COALESCE(array_agg(DISTINCT COALESCE(s.ssg_skill_id, 'mcf:' || s.norm_name))
                FILTER (WHERE s.skill_key IS NOT NULL), '{}') AS skills
FROM clean.dim_job j
LEFT JOIN clean.bridge_job_skill b ON b.job_key = j.job_key
LEFT JOIN clean.dim_skill s ON s.skill_key = b.skill_key
WHERE j.is_current AND NOT j.salary_is_hidden AND j.salary_min > 0 AND j.salary_max >= j.salary_min
  AND j.salary_mid BETWEEN 300 AND 150000
GROUP BY j.job_key
"""

SKILL_LABEL_SQL = """
SELECT COALESCE(ssg_skill_id, 'mcf:' || norm_name), min(COALESCE(ssg_skill_title, mcf_skill_name))
FROM clean.dim_skill GROUP BY 1
"""

KEEP_RUNS = 5


def _load_rows(conn):
    with conn.cursor() as cur:
        cur.execute(TRAINING_SQL, {"test": TEST_PERCENT})
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        cur.execute(SKILL_LABEL_SQL)
        skill_labels = dict(cur.fetchall())
    return rows, skill_labels


def save_model(conn, model: SalaryModel, snapshot_date) -> int:
    from psycopg2.extras import execute_values
    m = model.metrics
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ml.salary_model_run (snapshot_date, n_train, n_test, n_features, alpha, intercept, mae, mape,
                                             median_ape, within_15, baseline_mape, resid_p10, resid_p90)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id
        """, (snapshot_date, m["n_train"], m["n_test"], m["n_features"], m["alpha"], model.intercept, m["mae"],
              m["mape"], m["median_ape"], m["within_15"], m["baseline_mape"], model.resid_p10, model.resid_p90))
        run_id = cur.fetchone()[0]
        execute_values(cur, "INSERT INTO ml.salary_model_coef (run_id, feature, kind, label, coef, support) VALUES %s",
                       [(run_id, name, feature_kind(name), model.labels.get(name, name), coef,
                         model.support.get(name, 0)) for name, coef in model.coefs.items()])
        cur.execute("DELETE FROM ml.salary_model_run WHERE run_id <= %s", (run_id - KEEP_RUNS,))
    return run_id


def load_model(conn) -> "SalaryModel | None":
    """The most recent stored model, or None if nothing was trained yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ml.salary_model_run') IS NOT NULL")
        if not cur.fetchone()[0]:
            return None
        cur.execute("SELECT * FROM ml.salary_model_run ORDER BY run_id DESC LIMIT 1")
        run = cur.fetchone()
        if run is None:
            return None
        run = dict(zip([d[0] for d in cur.description], run))
        cur.execute("SELECT feature, label, coef, support FROM ml.salary_model_coef WHERE run_id = %s",
                    (run["run_id"],))
        coefs = cur.fetchall()
    metrics = {k: run[k] for k in ("n_train", "n_test", "n_features", "alpha", "mae", "mape", "median_ape",
                                   "within_15", "baseline_mape")}
    metrics.update(run_id=run["run_id"], trained_at=run["trained_at"], snapshot_date=run["snapshot_date"])
    return SalaryModel(
        intercept=run["intercept"], coefs={f: c for f, _l, c, _s in coefs},
        labels={f: l for f, l, _c, _s in coefs}, support={f: s for f, _l, _c, s in coefs},
        resid_p10=run["resid_p10"], resid_p90=run["resid_p90"], metrics=metrics)


def train(conn) -> dict:
    """Train on the warehouse and store the model. Returns the metrics, or {"skipped": reason} if there is
    not enough salary data yet (a skipped run must not fail the daily DAG)."""
    started = time.perf_counter()
    with conn, conn.cursor() as cur:
        cur.execute(MODEL_SQL.read_text(encoding="utf-8"))
    rows, skill_labels = _load_rows(conn)
    train_rows = [r for r in rows if not r["is_test"]]
    test_rows = [r for r in rows if r["is_test"]]
    if len(train_rows) < MIN_TRAIN_ROWS or len(test_rows) < MIN_TEST_ROWS:
        return {"skipped": f"needs at least {MIN_TRAIN_ROWS} training and {MIN_TEST_ROWS} test postings with a "
                           f"salary, found {len(train_rows)} and {len(test_rows)}"}
    model = fit(train_rows, test_rows)
    for name in model.labels:
        if name.startswith("skill:"):
            model.labels[name] = skill_labels.get(name[len("skill:"):], name[len("skill:"):])
    with conn, conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_date) FROM clean.fact_job_day")
        snapshot_date = cur.fetchone()[0]
    with conn:
        run_id = save_model(conn, model, snapshot_date)
    return {"run_id": run_id, "seconds": round(time.perf_counter() - started, 1), **model.metrics}


if __name__ == "__main__":
    from transform.db import connect

    connection = connect()
    try:
        print(train(connection))
    finally:
        connection.close()
