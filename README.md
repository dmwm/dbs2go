### dbs2go package

![Build Status](https://github.com/dmwm/dbs2go/actions/workflows/go.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/dmwm/dbs2go)](https://goreportcard.com/report/github.com/dmwm/dbs2go)
[![GoDoc](https://godoc.org/github.com/dmwm/dbs2go?status.svg)](https://godoc.org/github.com/dmwm/dbs2go)

The `dbs2go` package represents Data Bookkeeping Server (DBS) used
by CMS collaboration. Please refer to the appropriate section of `dbs2go`
documentation:

- [Installation instruction](docs/Installation.md)
  - [DBS deployment on CMSWEB k8s cluster](docs/k8s.md)
- [DBS Server architecture](docs/DBSServer.md)
  - [DBS Reader](docs/DBSReader.md)
  - [DBS Writer](docs/DBSWriter.md)
  - [DBS Migration server](docs/MigrationServer.md)
  - [Repository structure and code logic](https://github.com/dmwm/dbs2go/blob/master/docs/DBSServer.md#repository-structure-and-code-logic)
  - [DBS business and DAO logic](https://github.com/dmwm/dbs2go/blob/master/docs/DBSServer.md#dbs-business-and-dao-logic)
  - [DBS errors](https://github.com/dmwm/dbs2go/blob/master/docs/DBSServer.md#dbs-errors)
- [DBS client](docs/Client.md)
- [DBS APIs](docs/apis.md)
- [Debugging and Profiling DBS server](docs/Debug.md)
- [DBS GraphQL](graphql/README.md)

#### Release, upload, and k8s deployment

The release flow can be executed manually in three steps:

```
make release v1.2.3
make upload TAG=v1.2.3
make k8deploy TAG=v1.2.3 IMAGEBOT_URL=<imagebot-url>
```

`make release <tag>` calls `buildRelease.sh`, updates the changelog, creates
the release commit and tag, and pushes them to the configured git remote.

`make upload` builds the local `dbs2go` binary with Oracle support enabled by
default, builds the Docker image, and pushes it to
`registry.cern.ch/cmsweb/dbs2go`. Stable release tags also push a
`<tag>-stable` image tag.

`make k8deploy` runs the imagebot deployment step. The repository passed to
imagebot is resolved from the `upstream` git remote, then `origin`, and finally
falls back to `dmwm/dbs2go`. It can be overridden with `REPO=<owner>/<repo>`.

For builds without Oracle drivers, use either form:

```
make upload no-oracle TAG=v1.2.3
make upload-no-oracle TAG=v1.2.3
```

The k8s deployment target accepts the same suffix for command consistency,
although it does not build anything after the upload/deploy split:

```
make k8deploy no-oracle TAG=v1.2.3 IMAGEBOT_URL=<imagebot-url>
make k8deploy-no-oracle TAG=v1.2.3 IMAGEBOT_URL=<imagebot-url>
```
