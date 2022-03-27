The [GraphQL](https://graphql.org/) queries in DBS are supported
as proof-of-concept. The implementation
is done via [graph-gophers](https://github.com/graph-gophers/graphql-go)
library. The code reads
[DBS GraphQL schema](https://github.com/dmwm/dbs2go/blob/master/static/schema/schema.graphql)
and provides `/query` end-point for GraphQL queries. The queries
can be posted via HTTP POST request as following:
```
curl -X POST -d@/tmp/qraph.ql https://some-host.com/dbs2go/query
```
where `/tmp/graph.ql` represents GraphQL query like
```
{"query": "{getDataset(name: \"test\") {name}}"}
```
