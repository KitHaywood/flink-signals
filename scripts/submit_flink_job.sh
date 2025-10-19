#!/usr/bin/env bash
set -euo pipefail

# Placeholder script for submitting PyFlink jobs to the JobManager.
# Usage: ./scripts/submit_flink_job.sh <job.py>

JOB_FILE=${1:-/opt/flink/usrlib/__main__.py}
FLINK_BIN=${FLINK_HOME:-/opt/flink}/bin/flink

echo "Submitting job ${JOB_FILE} to ${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
"${FLINK_BIN}" run -m "${FLINK_JOBMANAGER_HOST:-flink-jobmanager}:8081" "${JOB_FILE}"
