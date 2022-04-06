# Example PySpark Job for Neo4j

This job assumes you're using Google DataProc's managed PySpark environment.

## Prerequisites

You'll need a functioning Google Cloud SDK installed and available as `gcloud`.

> Windows users...just use WSL2. Save yourself the trouble.

Create a Python virtual environment:

```
$ python -m venv venv
$ . venv/bin/activate
```

Install requirements:

```
(venv)$ pip install -r requirements.txt
```

_et voila_

## Usage

I've wrapped most of the complexity of submitting the job into `run.sh`.

```
usage: run.sh [pyspark file]

required environment config (cause I'm lazy!):
    STAGING: GCS location for staging job artifacts
    CLUSTER: Dataproc cluster to run the job
     BUCKET: Name of the GCS source bucket
  NEO4J_URL: Neo4j bolt url (e.g. neo4j://hostname:7687)
```

Peek into `run.sh` for other environment settings.
