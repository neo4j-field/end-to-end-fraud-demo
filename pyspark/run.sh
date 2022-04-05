#!/bin/sh
STAGING="gs://neo4j_voutila/spark"

REGION="northamerica-northeast1"
CLUSTER="voutila-dp-test"
PROJECT="neo4j-se-team-201905"

JARFILE="neo4j-connector-apache-spark_2.12-4.1.0_for_spark_3.jar"
JOBFILE="load.py"

# TODO: check if in VENV

# deploy Python job
gsutil cp "${JOBFILE}" "${STAGING}"

# package up env
venv-pack -f --compress-level 2

# deploy virtual environment
gsutil -m -o "GSUtil:parallel_composite_upload_threshold=64" \
       cp \
       "venv.tar.gz" "${STAGING}/"

# deploy Neo4j Spark Connector
gsutil cp "${JARFILE}" "${STAGING}/"

# submit job
gcloud dataproc jobs submit pyspark \
       --region "${REGION}" \
       --cluster "${CLUSTER}" \
       --jars "${STAGING}/${JARFILE}" \
       "${STAGING}/${JOBFILE}"
