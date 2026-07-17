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
SUBMIT_CMD=(
    spark-submit
    --master          "${SPARK_MASTER}"
    --name            "${SPARK_APP_NAME}"
    --driver-memory   "${SPARK_DRIVER_MEMORY}"
    --executor-memory "${SPARK_EXECUTOR_MEMORY}"
    --executor-cores  "${SPARK_EXECUTOR_CORES}"
    --conf "spark.ui.enabled=true"
    --conf "spark.ui.port=4040"
    # When running on k8s, the driver needs to advertise its own address so
    # executors can call back to it.  HOSTNAME is automatically set by k8s.
    --conf "spark.driver.host=${HOSTNAME:-localhost}"
)

# Append any extra args supplied at runtime
if [[ -n "${SPARK_EXTRA_ARGS}" ]]; then
    # Word-split intentional here — SPARK_EXTRA_ARGS is a space-separated list
    # shellcheck disable=SC2086
    SUBMIT_CMD+=(${SPARK_EXTRA_ARGS})
fi

# Append the job file last (positional argument to spark-submit)
SUBMIT_CMD+=("${SPARK_JOB_FILE}")

# ── Execute ───────────────────────────────────────────────────────────────────
log "Executing: ${SUBMIT_CMD[*]}"
exec "${SUBMIT_CMD[@]}"
