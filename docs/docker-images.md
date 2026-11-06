# Docker images: our own, pinned

Written Fri 6 Nov 2026. Files: `docker/airflow/Dockerfile`, `docker/airflow/requirements.txt`, `dashboard/Dockerfile`,
`dashboard/requirements.txt`, `.dockerignore`, `docker-compose.yml`, `requirements.txt`.

## 1. What changed

| | Before | Now |
|---|---|---|
| Airflow image | `apache/airflow:2.10.5` as is | `is3107-airflow:2.10.5-r1`, built from `docker/airflow/Dockerfile` (`FROM apache/airflow:2.10.5`) |
| openpyxl, rapidfuzz | pip-installed by the image's entrypoint at **every container start** (`_PIP_ADDITIONAL_REQUIREMENTS`, `>=` ranges) | installed **once at build time**, exact versions |
| Dashboard image | `python:3.11-slim`, `>=` ranges | `is3107-dashboard:r1`, `python:3.11.17-slim`, exact versions |
| Build context | whole repo sent to Docker, including `data/` (gigabytes of snapshots on the collector) | `.dockerignore` sends only the two requirements files |
| `requirements.txt` (laptops, tests) | `>=` ranges | exact versions the test suite passed with |

The code is still **mounted, not copied** into the images, so `git pull` reaches the scheduler and the dashboard
without a rebuild. Only a change to `docker/airflow/` or the dashboard's requirements needs one.

## 2. Why (wk11)

- **Build time vs run time** (wk11 [41], [58]). Installing at start means every `docker compose up` needs PyPI,
  each of the three Airflow containers spends time on pip before it is healthy, and a restart on a bad network
  can leave the transform without openpyxl (the `load_reference_tables` task fails) or without rapidfuzz (skill
  matching silently falls back to difflib and gives different matches). At build time it happens once, and a
  failed install fails the build, not the 10:30 run. The Dockerfile's last step imports every package the DAGs
  need, so a missing one stops the build.
- **Pin versions: "latest is not always the greatest"** (wk11 [92]). The `>=` ranges were a live example: an
  unpinned dashboard build on 6 Nov 2026 resolved to **pandas 3.1.1**, a major version the dashboard had never run
  with (the tests pass on pandas 2.3.3). The pins are what works today: for Airflow exactly what the start-time
  install resolved to on 6 Nov (openpyxl 3.1.5, et-xmlfile 2.0.0, rapidfuzz 3.14.6; numpy 1.26.4 and pyarrow
  19.0.0 come with the base image), for the dashboard the versions every tab was rendered with on 6 Nov.
- **"Never do that before a real demo"** (wk11 [92]): after the 15 Nov freeze nobody bumps a pin.
- **Write your own Dockerfile "as a challenge"** (wk11 [60]): a short, commented one that extends the official
  image the documented way (install as the `airflow` user, and name `apache-airflow==2.10.5` in the same pip
  command so pip can never change Airflow itself).
- Base image pinned by tag. The digest of the `apache/airflow:2.10.5` we tested with is
  `sha256:6499a680a93463846d3a6be980e85d601dc97b0d81e82eed9ef5e5cb9da31b79`. It is not pinned by digest in the
  Dockerfile because a laptop that pulled 2.10.5 on another day may hold a different digest and would then
  download 2 GB again.

**Image tags as a version switch.** The Airflow image is `is3107-airflow:2.10.5-r1` and the dashboard
`is3107-dashboard:r1`. Whoever changes a Dockerfile or its requirements bumps `r1` to `r2` in
`docker-compose.yml` in the same commit. Compose then finds no image with that tag and builds it on the next
`docker compose up -d`, so no laptop keeps running a stale image by accident.

## 3. Measured (6 Nov 2026, Brian's laptop)

| | Number |
|---|---|
| `docker compose build airflow-init dashboard` (both images, base images already pulled) | 114 s in all; the Airflow pip layer itself 3.5 s |
| Build context sent to Docker | under 1.1 KB per image (`.dockerignore`), instead of the whole repo with `data/` |
| Image sizes | `is3107-airflow:2.10.5-r1` 2.16 GB (base 2.13 GB), `is3107-dashboard:r1` 938 MB |
| One container start to a ready Python with openpyxl and rapidfuzz | **13.6 s** old way (entrypoint pip install, needs PyPI) vs **6.3 s** new. The compose stack starts three such containers |
| Versions inside the built Airflow image | numpy 1.26.4, openpyxl 3.1.5, pyarrow 19.0.0, rapidfuzz 3.14.6 (printed by the build's last step) |
| DAG import check in the built image | `airflow dags list-import-errors` in a throwaway `is3107-airflow:2.10.5-r1` container with the repo folders mounted as in compose: "No data found" (no errors), all five DAGs listed |
| Dashboard in the built image | every tab rendered with no exception against a warehouse holding the real 24 and 25 Oct snapshots (Streamlit AppTest, headless), and `tests/test_dashboard_marts.py`, `test_dashboard_logic.py`, `test_integration_ops.py` passed inside it (29 tests) |

## 4. Deploy on the collector laptop (migration)

Do it at a quiet time: not between 09:55 and the end of the 10:00 crawl, and not right before a :00 or :30 live
poll. The warehouse volume and its data are not touched. **Never run `docker compose down -v`.**

```bash
git pull
docker compose build              # about 1 to 2 minutes; needs internet once. The running stack keeps running.
docker compose up -d              # recreates airflow-init, webserver, scheduler (and dashboard) on the new images
docker compose ps                 # all Up; airflow-init "Exited (0)" is normal
docker compose exec airflow-scheduler python -c "import openpyxl, rapidfuzz; print(openpyxl.__version__, rapidfuzz.__version__)"
#   expect: 3.1.5 3.14.6
docker compose logs airflow-scheduler | grep -i "pip install"   # expect nothing: no install at start any more
```

If `docker compose up -d` is run without `build` first, compose builds the missing image itself. If something is
wrong, the old image is still on the laptop: `git checkout` the previous `docker-compose.yml` and
`docker compose up -d` brings back `apache/airflow:2.10.5` with the start-time install.

Running tasks: `up -d` restarts the scheduler, so a task running at that moment is marked failed and retried
(each DAG has retries). That is why the timing above matters for the crawl.
