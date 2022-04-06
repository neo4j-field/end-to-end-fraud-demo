# dump2csv

Clojure utility for dumping data from a Neo4j system via a Cypher query.

## Configuration
Expects environment variables because I'm lazy:

- `NEO4J_URL`: bolt url (default: `neo4j://localhost:7687`)
- `NEO4J_USER`: username (default: `neo4j`)
- `NEO4J_PASS`: password (default: `password`)

## Usage

To build or use, go grab [Leiningen](https://leiningen.org/).

```
$ lein run "MATCH (n) RETURN id(n)"
```

That's it! It dumps the fields returned to `stdout` without any headers/fieldnames.
