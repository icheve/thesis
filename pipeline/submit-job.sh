#!/bin/bash
set -e
echo "Waiting for Flink JobManager..."
until curl -sf http://flink-jobmanager:8081/overview; do sleep 3; done
echo "Submitting PyFlink job..."
flink run --jobmanager flink-jobmanager:8081 \
  -py /opt/flink/pipeline/main.py \
  -pyfs /opt/flink \
  -pyreq /opt/flink/pipeline/requirements.txt \
  -p "${FLINK_PARALLELISM:-2}" \
  ${FLINK_JOB_ARGS:-}
echo "Job submitted."
