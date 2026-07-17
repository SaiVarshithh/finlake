# ==============================================================================
# FinLake — PySpark Image  (single Dockerfile)
#
# Strategy: pip install pyspark bundles the full Spark binaries + spark-submit.
# No separate tarball download needed — keeps the image lean and build simple.
#
# Base   : python:3.11-slim  (~130 MB)
# + JRE  : openjdk-17-jre-headless (~200 MB)
# + pip  : pyspark + job deps      (~500 MB)
# Total  : ~830 MB  (vs ~1.2 GB for bitnami/spark)
# ==============================================================================

FROM python:3.11-slim

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# ── Python / Spark deps ───────────────────────────────────────────────────────
# pip install pyspark ships spark-submit, pyspark shell, and all JARs inside
# the package — no need to download a separate Spark tarball.
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Symlink pyspark install location → /opt/spark so SPARK_HOME is stable
# regardless of the Python minor version used in the base image.
RUN ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark

# ── Spark environment ─────────────────────────────────────────────────────────
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Runtime defaults — override via k8s ConfigMap / -e flags
ENV SPARK_MASTER="local[*]"
ENV SPARK_APP_NAME="finlake-spark-job"
ENV SPARK_JOB_FILE="/opt/spark-jobs/sample_job.py"
ENV SPARK_DRIVER_MEMORY="1g"
ENV SPARK_EXECUTOR_MEMORY="1g"
ENV SPARK_EXECUTOR_CORES="1"
ENV SPARK_EXTRA_ARGS=""

# ── Non-root user ─────────────────────────────────────────────────────────────
RUN useradd -m -u 1001 -s /bin/bash spark

# ── Job files ─────────────────────────────────────────────────────────────────
COPY --chown=spark:spark spark/jobs/ /opt/spark-jobs/
COPY --chown=spark:spark spark/entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

WORKDIR /opt/spark-jobs
USER 1001

EXPOSE 4040 7077

# tini as PID-1 — cleanly reaps Spark child processes on container stop
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/spark/entrypoint.sh"]