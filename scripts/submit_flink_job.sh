#!/usr/bin/env bash
set -euo pipefail

# Placeholder script for submitting PyFlink jobs to the JobManager.
# Usage: ./scripts/submit_flink_job.sh <job.py>

JOB_FILE=${1:-/opt/flink/usrlib/__main__.py}
RUN_FLAGS=${FLINK_RUN_FLAGS:-}
FLINK_BIN=${FLINK_HOME:-/opt/flink}/bin/flink

echo "Submitting job ${JOB_FILE} to ${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
if [[ -n "${RUN_FLAGS}" ]]; then
  # shellcheck disable=SC2086
  "${FLINK_BIN}" run ${RUN_FLAGS} -m "${FLINK_JOBMANAGER_HOST:-flink-jobmanager}:8081" "${JOB_FILE}"
else
  "${FLINK_BIN}" run -m "${FLINK_JOBMANAGER_HOST:-flink-jobmanager}:8081" "${JOB_FILE}"
fi
