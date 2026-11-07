-- Parquet lake export log: one row per dataset partition written by transform/lake_export.py (the weekly_lake_export
-- DAG or its CLI). The lake itself is files under data/lake/ (docs/lake-and-spark.md); this table records what was
-- written, how big it is and how long it took, for the scale and cost numbers (docs/scale-and-cost.md).
--
-- Upserted on (dataset, partition_key), so re-exporting a day overwrites its row, as the export overwrites its file.
-- A day that has no rows when re-exported loses its row, as it loses its file.
-- This file is idempotent (IF NOT EXISTS). It runs on first container start after 01 to 11. On an existing warehouse
-- apply it by hand before unpausing weekly_lake_export.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.lake_exports (
    dataset        TEXT        NOT NULL,      -- 'fact_job_day' (partitioned by snapshot_date) or 'dim_skill' (one file)
    partition_key  TEXT        NOT NULL,      -- '2026-10-24', or 'all' for an unpartitioned dataset
    rows           INTEGER     NOT NULL,
    bytes          BIGINT      NOT NULL,      -- size of the Parquet file on disk
    seconds        NUMERIC(10,3) NOT NULL,    -- query plus write
    path           TEXT        NOT NULL,      -- relative to the lake root
    exported_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, partition_key)
);
