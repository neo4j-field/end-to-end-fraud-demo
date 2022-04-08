#!/bin/sh

# Current Neo4j Spark Connector jar
JARFILE="neo4j-connector-apache-spark_2.12-4.1.1_for_spark_3.jar"

# GCP config
STAGING="${STAGING:=}"
REGION="${REGION:=$(gcloud config get dataproc/region)}"
CLUSTER="${CLUSTER:=}"
PROJECT="${PROJECT:=$(gcloud config get project)}"

# Source bucket details
BUCKET="${BUCKET:=}"
PREFIX="${PREFIX:=}"

# Neo4j config
NEO4J_URL="${NEO4J_URL:=}"
NEO4J_USER="${NEO4J_USER:=neo4j}"
NEO4J_PASS="${NEO4J_PASS:=password}"
DATABASE="${DATABASE:=neo4j}"

usage () {
    echo "usage: run.sh [pyspark file]" >&2
    echo "" >&2
    echo "required environment config (cause I'm lazy!):" >&2
    echo "    STAGING: GCS location for staging job artifacts" >&2
    echo "    CLUSTER: Dataproc cluster to run the job" >&2
    echo "     BUCKET: Name of the GCS source bucket" >&2
    echo "  NEO4J_URL: Neo4j bolt url (e.g. neo4j://hostname:7687)" >&2
    exit 1
}

fail () {
    msg="${1:=unknown error}"
    echo "Uh oh! ${msg}" >&2
    usage # dead
}

# Our only positional argument
if [ "${#}" -ne 1 ]; then usage; fi
JOBFILE="${1:=}"

# Inspect required toggles
if [ ! -f "${JOBFILE}" ]; then usage; fi
if [ -z "${STAGING}" ]; then fail "no STAGING set, please specify a GCS uri!"; fi
if [ -z "${CLUSTER}" ]; then fail "no CLUSTER set, please specify a cluster name"; fi
if [ -z "${NEO4J_URL}" ]; then fail "no NEO4J_URL set, please provide a Bolt URI"; fi
if [ -z "${BUCKET}" ]; then fail "no BUCKET set, please set to source of parquet file!"; fi


# Package up our virtual environment
if [ -n "${VIRTUAL_ENV:=}" ]; then
    venv-pack -f
else
    echo "Warning: couldn't determine if you're using a Python virtual environment!" >&2
fi

# Deploy our job artifacts using rsync
gsutil rsync -x "venv" "$(pwd)" "${STAGING}/"

# Submit DataProc job!
gcloud dataproc jobs submit pyspark \
       --region "${REGION}" \
       --cluster "${CLUSTER}" \
       --jars "${STAGING}/${JARFILE}" \
       "${STAGING}/${JOBFILE}" -- \
       --user "${NEO4J_USER}" --password "${NEO4J_PASS}" --prefix "${PREFIX}" \
       --database "${DATABASE}" \
       "${NEO4J_URL}" "${BUCKET}"
