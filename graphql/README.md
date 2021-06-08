This folder contains a proof-of-concept for support
[GraphQL](https://graphql.org/) queries for DBS. The implementation
is done via [graph-gophers](https://github.com/graph-gophers/graphql-go)
library. The code reads
[DBS GraphQL schema](https://github.com/vkuznet/dbs2go/blob/master/static/schema/schema.graphql)
and provides `/query` end-point to place GraphQL queries. The queries
can be posted via HTTP POST request as following:
```
curl -XPOST -d@/tmp/qraph.ql http://localhost:8989/dbs2go/query
```
where `/tmp/graph.ql` represents GraphQL query like
```
{"query": "{getDataset(name: \"test\") {name}}"}
```
and talks to DBS server running on localhost on port 8989 under
`/dbs2go` path.
