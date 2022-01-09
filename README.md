# twitter-network-search
Uses the Twitter V2 REST API to search for Twitter entities and save to CSV files. Intended to be imported into a Neo4j database.
Requires the Bearer Token to be set before running eg
`set BEARER_TOKEN=$BEARER_TOKEN`

Once the CSV files are created, the cypher queries in the file `neo4j_import_script`.