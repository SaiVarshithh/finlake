# ==============================================================================
# FinLake — PySpark Image
# Python 3.11 + Java 21
# ==============================================================================

FROM python:3.11-slim

# ------------------------------------------------------------------------------
# Install Java + system utilities
# ------------------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre \
        procps \
        tini && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# ------------------------------------------------------------------------------
# Java 21 + Spark 3.5.x module-access fix
# Spark reflectively touches sun.nio.ch.DirectBuffer and other JDK internals
# that JPMS blocks by default from Java 9+. spark-submit's own command
# builder injects these automatically for the driver, but code paths that
# invoke spark-class/java directly (e.g. our executor branch in entrypoint.sh)
# do NOT get them for free. Setting JDK_JAVA_OPTIONS applies it to every JVM
# launch in this image, driver and executor alike, regardless of code path.
# ------------------------------------------------------------------------------
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

# ------------------------------------------------------------------------------
# Install Python dependencies
# ------------------------------------------------------------------------------
COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ------------------------------------------------------------------------------
# Configure Spark
# ------------------------------------------------------------------------------
RUN ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ------------------------------------------------------------------------------
# Runtime Defaults
# ------------------------------------------------------------------------------
ENV SPARK_MASTER="local[*]"
ENV SPARK_APP_NAME="finlake-spark-job"
ENV SPARK_JOB_FILE="/opt/spark-jobs/sample_job.py"
ENV SPARK_DRIVER_MEMORY="1g"
ENV SPARK_EXECUTOR_MEMORY="1g"
ENV SPARK_EXECUTOR_CORES="1"
ENV SPARK_EXTRA_ARGS=""

# ------------------------------------------------------------------------------
# Create non-root user
# ------------------------------------------------------------------------------
RUN useradd -m -u 1001 -s /bin/bash spark

# ------------------------------------------------------------------------------
# Copy application
# ------------------------------------------------------------------------------
COPY --chown=spark:spark spark/jobs/ /opt/spark-jobs/
COPY --chown=spark:spark spark/entrypoint.sh /opt/spark/entrypoint.sh

RUN chmod +x /opt/spark/entrypoint.sh

WORKDIR /opt/spark-jobs

USER 1001

EXPOSE 4040
EXPOSE 7077

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/spark/entrypoint.sh"]