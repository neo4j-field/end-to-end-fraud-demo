# end-to-end-fraud-demo

- `dump2csv`: Clojure tool I used to quickly extract data from the original Neo4j database to build the csv files
- `to_parquet.py`: Python script for converting csv files into parquet files using `PyArrow`
- `pyspark`: `PySpark` job for loading parquet files into Neo4j
- `fraud-detection-demo-with-p2p-dv.ipynb`: updated demo IPython notebook

For background, see the original demo blog series:
https://neo4j.com/developer-blog/exploring-fraud-detection-neo4j-graph-data-science-part-1/
