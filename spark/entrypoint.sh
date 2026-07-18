#!/usr/bin/env bash
# ==============================================================================
# FinLake — PySpark Entrypoint
# Submits a PySpark application via spark-submit.
#
# All configuration is driven by environment variables so that this image can
# be re-used for any job without rebuilding it — just point to the right job
# file and tune the memory/core settings from your k8s Job/CronJob manifest.
#
# Key environment variables:
#   SPARK_MASTER         — e.g. "local[*]" | "k8s://https://<api-server>" | "spark://<master>:7077"
#   SPARK_APP_NAME       — Logical name shown in the Spark UI
#   SPARK_JOB_FILE       — Path to the .py entry point inside the container
#   SPARK_DRIVER_MEMORY  — Driver heap size  (default: 1g)
#   SPARK_EXECUTOR_MEMORY— Executor heap size (default: 1g)
#   SPARK_EXECUTOR_CORES — Cores per executor (default: 1)
#   SPARK_EXTRA_ARGS     — Any additional spark-submit flags (space-separated)
# ==============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_APP_NAME="${SPARK_APP_NAME:-finlake-spark-job}"
SPARK_JOB_FILE="${SPARK_JOB_FILE:-/opt/spark-jobs/sample_job.py}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
SPARK_EXTRA_ARGS="${SPARK_EXTRA_ARGS:-}"

# ── Logging helpers ───────────────────────────────────────────────────────────
log()  { echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ') | $*"; }
die()  { log "ERROR: $*" >&2; exit 1; }

# ── Spark Executor / Internal Command Bypass ──────────────────────────────────
# In k8s native mode, Spark starts executor pods with a single arg "executor"
# and injects connection details as environment variables. These are the ACTUAL
# names Spark's KubernetesExecutorBuilder sets (verified against Spark 3.5.x
# source, resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh):
#   SPARK_DRIVER_URL, SPARK_EXECUTOR_ID, SPARK_EXECUTOR_CORES,
#   SPARK_APPLICATION_ID, SPARK_EXECUTOR_POD_IP, SPARK_RESOURCE_PROFILE_ID (3.4+)
# There is no SPARK_EXECUTOR_DRIVER_URL / SPARK_EXECUTOR_HOSTNAME / SPARK_EXECUTOR_BIND_ADDRESS.
if [[ "$#" -gt 0 ]]; then
    if [[ "$1" == "executor" ]]; then
        log "Running Spark executor backend (reading Spark-injected env vars)..."
        exec /opt/spark/bin/spark-class \
            org.apache.spark.executor.CoarseGrainedExecutorBackend \
            --driver-url        "${SPARK_DRIVER_URL}" \
            --executor-id       "${SPARK_EXECUTOR_ID}" \
            --hostname          "${SPARK_EXECUTOR_POD_IP}" \
            --cores             "${SPARK_EXECUTOR_CORES:-1}" \
            --app-id            "${SPARK_APPLICATION_ID}" \
            --resourceProfileId "${SPARK_RESOURCE_PROFILE_ID:-0}"
    elif [[ "$1" == *"/spark-class" || "$1" == *"/spark-submit" ]]; then
        log "Executing Spark command directly: $@"
        exec "$@"
    fi
fi

# ── Validate job file exists ──────────────────────────────────────────────────
[[ -f "${SPARK_JOB_FILE}" ]] || die "Job file not found: ${SPARK_JOB_FILE}"

log "=========================================================="
log "  FinLake PySpark Job Runner"
log "=========================================================="
log "  Master         : ${SPARK_MASTER}"
log "  App name       : ${SPARK_APP_NAME}"
log "  Job file       : ${SPARK_JOB_FILE}"
log "  Driver memory  : ${SPARK_DRIVER_MEMORY}"
log "  Executor memory: ${SPARK_EXECUTOR_MEMORY}"
log "  Executor cores : ${SPARK_EXECUTOR_CORES}"
[[ -n "${SPARK_EXTRA_ARGS}" ]] && log "  Extra args     : ${SPARK_EXTRA_ARGS}"
log "=========================================================="

# ── Build spark-submit command ────────────────────────────────────────────────
# MY_POD_IP is injected as a real env var by the k8s Downward API (status.podIP).
# Expanding it here (inside the bash array) is the only safe way — no eval tricks.
SUBMIT_CMD=(
    spark-submit
    --master          "${SPARK_MASTER}"
    --name            "${SPARK_APP_NAME}"
    --driver-memory   "${SPARK_DRIVER_MEMORY}"
    --executor-memory "${SPARK_EXECUTOR_MEMORY}"
    --executor-cores  "${SPARK_EXECUTOR_CORES}"
    --conf "spark.ui.enabled=false"
    --conf "spark.driver.port=7077"
    # MY_POD_IP → real pod IP (Downward API). Falls back to HOSTNAME for local[*] mode.
    --conf "spark.driver.host=${MY_POD_IP:-${HOSTNAME:-localhost}}"
)

# Append any extra k8s / package flags from the ConfigMap.
# SPARK_EXTRA_ARGS is a plain space-separated list — word-split is intentional.
if [[ -n "${SPARK_EXTRA_ARGS}" ]]; then
    # shellcheck disable=SC2086
    SUBMIT_CMD+=(${SPARK_EXTRA_ARGS})
fi

# Append the job file last (positional argument to spark-submit)
SUBMIT_CMD+=("${SPARK_JOB_FILE}")

# ── Execute ───────────────────────────────────────────────────────────────────
log "Executing: ${SUBMIT_CMD[*]}"
exec "${SUBMIT_CMD[@]}"
