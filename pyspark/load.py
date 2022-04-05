#!/usr/bin/env python3
from itertools import chain

import pyspark
from pyspark.sql import SparkSession
import pyarrow as pa

from google.cloud import storage

## Environment Config
BUCKET = "neo4j_voutila"
PREFIX = "fraud-demo"
NEO4J_URL = "neo4j://voutila-dataproc-demo.northamerica-northeast1-a.c.neo4j-se-team-201905.internal:7687"

NEO4J_SPARK_OPTS = {
    "url": NEO4J_URL,
    "authentication.type": "basic",
    "authentication.basic.username": "neo4j",
    "authentication.basic.password": "password",
}

## Cheat Sheet
TYPE = "relationship"
SOURCE = "relationship.source.labels"
SOURCE_KEY = "relationship.source.node.keys"
SOURCE_MODE = "relationship.source.save.mode"
TARGET = "relationship.target.labels"
TARGET_KEY = "relationship.target.node.keys"
TARGET_MODE = "relationship.target.save.mode"
SCHEMA = "schema.optimization.type"

def node(labels, key, **kwargs):
    return { "labels": labels, "node.keys": key, SCHEMA: "NODE_CONSTRAINTS" }

def edge(_type, source, source_key, target, target_key, props=None):
    options = {
        SOURCE: source, SOURCE_KEY: f"start_{source_key}:{source_key}", SOURCE_MODE: "Match",
        TARGET: target, TARGET_KEY: f"end_{target_key}:{target_key}", TARGET_MODE: "Match",
        TYPE: _type, "relationship.save.strategy": "keys", }
    if props:
        options.update({ "relationship.properties": props })
    return options

## Data Mapping
NODES = {
    "cards": node(":Cards", "guid"),
    "device": node(":Device", "guid"),
    "ip": node(":IP", "guid"),
    "user": node(":User", "guid"),
}
EDGES = {
    "has_cc": edge("HAS_CC", ":User", "guid", ":Card", "guid", "cardDate"),
    "has_ip": edge("HAS_IP", ":User", "guid", ":IP", "guid", "ipDate"),
    "p2p": edge("P2P", ":User", "guid", ":User", "guid", "totalAmount,transactionDateTime"),
    "referred": edge("REFERRED", ":User", "guid", ":User", "guid"),
    "used": edge("USED", ":User", "guid", ":Device", "guid", "deviceDate"),
}

## Globals
gcs = storage.Client()
spark = SparkSession.builder \
    .appName("Neo4j Fraud Demo with Spark") \
    .getOrCreate()

## Find our Source Data
blobs = gcs.list_blobs(BUCKET, prefix=PREFIX)
parquet_blobs = filter(lambda blob: blob.name.endswith('parquet'), blobs)

# Split our files into nodes and edges with their respective configuration
nodes, edges = [], []
for blob in parquet_blobs:
    basename = blob.name.replace(".parquet", "").replace(f"{PREFIX}/", "")
    uri = f"gs://{blob.bucket.name}/{blob.name}"
    if basename in NODES:
        nodes.append((uri, NODES[basename], "Overwrite"))
    elif basename in EDGES:
        edges.append((uri, EDGES[basename], "Append"))
    else:
        print(f"unknown file: {blob.name}")

# Make sure we operate on nodes first
for uri, config, mode in chain(nodes, edges):
    # Read the parquet file into a Spark DataFrame
    print(f"reading {uri}")
    df = spark.read.parquet(uri)
    print(f"read parquet file with {df.count():,} rows")

    # Use the Neo4j Spark Connector to write the Node/Edge to the database
    print(f"writing dataframe to {NEO4J_URL}")
    df.write \
      .format("org.neo4j.spark.DataSource") \
      .mode(mode) \
      .options(**NEO4J_SPARK_OPTS) \
      .options(**config) \
      .save()
    print(f"wrote {uri} to neo4j")
