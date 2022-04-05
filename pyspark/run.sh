#!/bin/sh
STAGING="${SPARK_STAGING:=gs://neo4j_voutila/spark}"
REGION="${REGION:=$(gcloud config get dataproc/region)}"
CLUSTER="${DATAPROC_CLUSTER:=}"
PROJECT="${PROJECT:=$(gcloud config get project)}"

BUCKET="${BUCKET:=}"
PREFIX="${PREFIX:=}"

NEO4J_URL="${NEO4J_URL:=}"
NEO4J_USER="${NEO4J_USER:=neo4j}"
NEO4J_PASS="${NEO4J_PASS:=password}"

fail () {
    msg="${1:=unknown error}"
    echo "Uh oh! ${msg}" >&2
    exit 1
}

if [ -z "${STAGING}" ]; then fail "no SPARK_STAGING set!"; fi
if [ -z "${CLUSTER}" ]; then fail "no DATAPROC_CLUSTER set!"; fi
if [ -z "${NEO4J_URL}" ]; then fail "no NEO4J_URL set!"; fi
if [ -z "${BUCKET}" ]; then fail "no BUCKET set, please set to source of parquet file!"; fi

JARFILE="neo4j-connector-apache-spark_2.12-4.1.0_for_spark_3-special.jar"
JOBFILE="load.py"

# Package up our virtual environment
if [ -n "${VIRTUAL_ENV}" ]; then
    venv-pack -f --compress-level 2
else
    echo "Warning: couldn't determine if you're using a virtual environment!" >&2
fi

# Deploy our job artifacts
gsutil rsync -x "venv" "$(pwd)" "${STAGING}/"

# submit job
gcloud dataproc jobs submit pyspark \
       --region "${REGION}" \
       --cluster "${CLUSTER}" \
       --jars "${STAGING}/${JARFILE}" \
       "${STAGING}/${JOBFILE}" -- \
       --user neo4j --password password --prefix fraud-demo \
       "${NEO4J_URL}" "${BUCKET}"
