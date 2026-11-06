# The team's Airflow image: the official 2.10.5 image plus our few pinned packages (docs/docker-images.md).
#
# Build time vs run time (wk11 [41], [58]): the packages are installed ONCE, here, when the image is built. Before,
# the official entrypoint pip-installed _PIP_ADDITIONAL_REQUIREMENTS every time a container started, which needs
# PyPI at every start, takes about a minute per container, and can pick up a different version on a different
# day ("latest is not always the greatest", wk11 [92]). The DAG and package code is NOT copied in: compose mounts
# it, so a git pull still reaches the scheduler without a rebuild.
#
# Build context is the repo root (docker-compose.yml); .dockerignore sends only the requirements files.
#   docker compose build        then        docker compose up -d
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY docker/airflow/requirements.txt /tmp/requirements-airflow.txt
# Install as the image's airflow user (the official way to extend it). Naming apache-airflow==<version> in the
# same command stops pip from upgrading or downgrading Airflow itself to satisfy a dependency.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

# Fail the build, not a DAG run at 10:00, if a package the DAGs import is missing.
RUN python -c "import numpy, openpyxl, psycopg2, pyarrow, rapidfuzz; print('numpy', numpy.__version__, '| openpyxl', openpyxl.__version__, '| pyarrow', pyarrow.__version__, '| rapidfuzz', rapidfuzz.__version__)"
