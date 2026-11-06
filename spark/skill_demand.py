"""Weekly skill demand from the Parquet lake, with PySpark (docs/lake-and-spark.md).

For every week (Monday start) and skill: how many distinct postings asked for it, on how many posting-days, its
share of the week's postings, and the applications those postings received that week. The same numbers as
spark/benchmark.py's Postgres query (SKILL_DEMAND_SQL there), which checks the two agree row for row.

Runs in the pinned apache/spark image, never started by default (compose profile "spark"):

    docker compose --profile spark run --rm spark          # reads data/lake, writes data/lake/_spark/...

Map and reduce in Spark terms: explode each posting-day's skill list (map), join the small dim_skill (broadcast,
no shuffle), then group by week and skill (reduce by key). Timings go to data/lake/_bench/spark_timing.json:
the JVM and session start-up once, then each run of the job (the first one reads the files cold).
"""
import argparse
import json
import os
import statistics
import time

T_PROCESS = time.perf_counter()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def build(spark, lake):
    fact = spark.read.parquet(os.path.join(lake, "fact_job_day"))
    skills = spark.read.parquet(os.path.join(lake, "dim_skill"))
    week = F.date_trunc("week", F.col("snapshot_date").cast("date")).cast("date")

    rows = (fact.select("snapshot_date", "job_uuid", F.coalesce("applications_delta", F.lit(0)).alias("apps"),
                        F.explode("skill_keys").alias("skill_key"))
            .join(F.broadcast(skills.select("skill_key", "skill_ref")), "skill_key")
            # two MCF tags can map to one SkillsFuture skill: count each posting-day once per skill
            .dropDuplicates(["snapshot_date", "job_uuid", "skill_ref"])
            .withColumn("week_start", week))
    per = rows.groupBy("week_start", "skill_ref").agg(
        F.countDistinct("job_uuid").alias("postings"),
        F.count(F.lit(1)).alias("posting_days"),
        F.sum("apps").alias("new_applications"))
    totals = (fact.withColumn("week_start", week).groupBy("week_start")
              .agg(F.countDistinct("job_uuid").alias("week_postings")))
    names = skills.groupBy("skill_ref").agg(F.min_by("skill_name", "skill_key").alias("skill_name"),
                                            F.max("is_mapped").alias("is_mapped"))
    return (per.join(totals, "week_start").join(F.broadcast(names), "skill_ref")
            .withColumn("share_of_postings", F.col("postings") / F.col("week_postings"))
            .select("week_start", "skill_ref", "skill_name", "is_mapped", "postings", "posting_days",
                    "new_applications", "week_postings", "share_of_postings"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default="/opt/lake")
    ap.add_argument("--out", help="default: <lake>/_spark/skill_demand_weekly")
    ap.add_argument("--timing", help="default: <lake>/_bench/spark_timing.json")
    ap.add_argument("--repeat", type=int, default=3, help="runs of the job in one session (first = cold)")
    args = ap.parse_args()
    out = args.out or os.path.join(args.lake, "_spark", "skill_demand_weekly")
    timing_path = args.timing or os.path.join(args.lake, "_bench", "spark_timing.json")

    t0 = time.perf_counter()
    spark = (SparkSession.builder.appName("weekly_skill_demand")
             .config("spark.sql.shuffle.partitions", "8")      # small data: the default 200 is mostly overhead
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    session_seconds = time.perf_counter() - t0

    runs = []
    for i in range(max(1, args.repeat)):
        t = time.perf_counter()
        result = build(spark, args.lake)
        result.coalesce(1).write.mode("overwrite").parquet(out)    # one small file; overwrite = idempotent
        runs.append(round(time.perf_counter() - t, 3))

    n_rows = spark.read.parquet(out).count()
    fact_rows = spark.read.parquet(os.path.join(args.lake, "fact_job_day")).count()
    timing = {
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "fact_rows": fact_rows,
        "result_rows": n_rows,
        "python_start_to_session_seconds": round(t0 - T_PROCESS + session_seconds, 3),
        "session_start_seconds": round(session_seconds, 3),
        "job_runs_seconds": runs,
        "first_run_seconds": runs[0],
        "warm_run_seconds_median": round(statistics.median(runs[1:]), 3) if len(runs) > 1 else None,
        "total_process_seconds": round(time.perf_counter() - T_PROCESS, 3),
        "out": out,
    }
    os.makedirs(os.path.dirname(timing_path), exist_ok=True)
    with open(timing_path, "w", encoding="utf-8") as f:
        json.dump(timing, f, indent=2)
    print(json.dumps(timing))
    spark.stop()


if __name__ == "__main__":
    main()
