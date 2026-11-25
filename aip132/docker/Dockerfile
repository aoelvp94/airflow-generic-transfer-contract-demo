# Airflow 3.4.0 plus the providers the AIP-132 scheduler run imports.
# The Dag and the aip132 package are copied in. A bind mount of those paths
# is not used: when the source is not visible to the daemon, Compose creates
# an empty directory and the Dag processor finds no files. `just aip132-e2e`
# stages `build/` before `docker compose build`. Versions match
# constraints-3.4.0/constraints-3.12.txt.
# gcsfs is pinned so the task can attach gcsfs.core (the HTTP client).
# The full google provider is not required for that path.
FROM apache/airflow:3.4.0

USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt" \
    "apache-airflow-providers-common-io==1.11.0" \
    "apache-airflow-providers-openlineage==2.21.0" \
    "s3fs==2026.9.0" \
    "gcsfs==2026.10.1"

COPY --chown=airflow:root build/aip132 /opt/airflow/aip132_src/aip132
COPY --chown=airflow:root build/aip132_s3_prefix_to_gcs.py /opt/airflow/dags/aip132_s3_prefix_to_gcs.py
