### dbs2go installation instruction
To build `dbs2go` executable we need the following components:
- [Go compiler](https://golang.org/)
- [ORACLE instant client libs/headers](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- `oci8.pc` appropriate oci file, e.g.
```
libdir=/opt/Oracle/instantclient_19_8/
includedir=/opt/Oracle/instantclient_19_8/sdk/include/

Name: oci8
Description: oci8 library
Version: 19.8
Cflags: -I${includedir}
Libs: -L${libdir} -lclntsh
```
After that, you can build `dbs2go` executable by calling `make`.

#### Additional instructions
`dbs2go` may use either SQLite or ORACLE databases as its backend.
To access ORACLE database we may use either:
- [ora](gopkg.in/rana/ora.v3) driver
- [oci8](https://github.com/mattn/go-oci8) driver

#### Installation instruction for ora Oracle driver

- Download oracle client libraries and sdk from Oracle web site
- Setup the environment and build ora.v3 package
```
export CGO_CFLAGS=-I/path/Oracle/instantclient_12_1/sdk/include
# for Linux
export CGO_LDFLAGS="-L/path/Oracle/instantclient_12_1/ -locci -lclntsh -lipc1 -lmql1 -lnnz12 -lclntshcore -lons"
# for OSX
export CGO_LDFLAGS="-L/opt/Oracle/instantclient_11_2/ -locci -lclntsh -lnnz11"
go get gopkg.in/rana/ora.v3
```

To use this driver we define dbfile with *ora* entry
```
ora oracleLogin/oraclePassword@DB
```


### Installation instructions for oci8 Oracle driver
We can use another driver ```https://github.com/mattn/go-oci8```
To install it, we need to create an oci8.pc file and put it elsewhere.
Then we must point *PKG_CONFIG_PATH* environment variable to location of
oci8.pc directory. Here is an example of oci8.pc driver

```
libdir=/path/Oracle/instantclient_12_1/
includedir=/path/Oracle/instantclient_12_1/sdk/include/

Name: oci8
Description: oci8 library
Version: 12.1
Cflags: -I${includedir}
Libs: -L${libdir} -lclntsh
```

To install the driver we simply do

```
go get github.com/mattn/go-oci8
```

To use this driver we define dbfile with *oci8* entry
```
oci8 oracleLogin/oraclePassword@DB
```

## Make targets

### Native builds

`make` or `make all` builds `./dbs2go` for the current host. On ARM, the
`all` target temporarily disables Oracle imports, builds, and restores the
source files. The existing `.IGNORE` behavior is part of that restoration
sequence.

The primary build targets are:

| Target | Function |
| --- | --- |
| `make build` | Build `./dbs2go` for the current host with the version from `git describe`, unless `VERSION` is supplied. |
| `make build_debug` | Build with Go compiler optimizations and inlining disabled. |
| `make build_no_oracle` | Temporarily disable Oracle drivers, build, and restore the affected source files. |
| `make build-no-oracle` | Alias for `build_no_oracle`. |
| `make build_osx` | Build `dbs2go_osx_x86` for Darwin. |
| `make build_osx_arm64` | Build `dbs2go_osx_arm64`. |
| `make build_linux` | Build `dbs2go_linux`. |
| `make build_power8` | Build `dbs2go_power8` for Linux/ppc64le. |
| `make build_arm64` | Build `dbs2go_arm64` for Linux/arm64. |
| `make build_all` | Run all native and cross-platform build targets above. |
| `make install` | Install the Go package with `go install`. |
| `make clean` | Run `go clean` and remove `pkg/`. |
| `make vet` | Run `go vet .`. |

### Automated Oracle environment for host builds

On a supported Linux host with Docker, prepare the Oracle client locally with:

```console
make oracle-env
```

This target pulls `registry.cern.ch/cmsweb/oracle:21_5-stable`, checks that its
architecture matches the host, extracts the client under
`.docker.build/oracle-env/oracle`, creates a host-specific `oci8.pc`, and
validates it with `pkg-config`. It does not install files system-wide.

To prepare that environment and build in one command, use:

```console
make build-ora
```

`make build_ora` is an alias. The architecture is checked again immediately
before compilation and build failures are propagated.

Run the resulting executable with the extracted Oracle shared libraries:

```console
LD_LIBRARY_PATH="$PWD/.docker.build/oracle-env/oracle${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
./dbs2go
```

`PKG_CONFIG_PATH` is required during compilation but not at runtime.

`make oracle-arch-check` repeats only the Linux/architecture comparison against
an already pulled `ORACLE_IMAGE`. `strip_oracle` and `restore_oracle` are
internal source-rewrite targets used by the restoration-aware ARM and
no-Oracle flows; they should normally be invoked through `all`, `test`,
`build_no_oracle`, or `test-errors-no-oracle` rather than directly.

### Docker images

The public Docker dispatcher requires an explicit third argument:

```console
make docker build localtree
make docker build dev
make docker build v1.2.3
make docker push localtree
make docker push dev
make docker push v1.2.3
```

`make docker build localtree` and its alias `make docker build dev` recreate
`.docker.build/src` from the current working tree, including uncommitted source changes. They exclude repository
metadata, Codex state, temporary trees, and generated build artifacts. The
source is compiled inside the CMSKubernetes `Dockerfile.dev` builder and
the image is tagged according to the supplied mode: `:localtree` or `:dev`.

`make docker build <release-tag>` downloads the regular CMSKubernetes Docker
assets and builds the requested tag inside the container. Accepted tags match
`v1.2.3`, `1.2.3`, and optional `rcN` suffixes. Omitted references and commit
IDs are rejected. All modes verify that the resulting image exists locally.

`make docker push <tag>` verifies the local image, logs in to the configured
registry, and pushes it. Stable release tags also produce and push
`<tag>-stable`; release candidates, `localtree`, and `dev` do not.

The image location defaults to:

```text
registry.cern.ch/cmsweb/dbs2go
```

Override it with `REGISTRY`, `PROJECT`, `REPOSITORY`, or `IMAGE`. Generated
Docker assets and staged source remain under the ignored `.docker.build/`
directory.

The lower-level targets are also available:

```console
make docker-build TAG=v1.2.3
make docker-push TAG=v1.2.3
```

### Release, upload, and deployment

A manual release flow is:

```console
make release v1.2.3
make docker build v1.2.3
make docker push v1.2.3
make k8deploy TAG=v1.2.3 IMAGEBOT_URL=<imagebot-url>
```

`make release <tag>` invokes `buildRelease.sh`, which performs the repository's
release, changelog, tag, and remote publication workflow.

`make upload TAG=<tag>` is the legacy wrapper that runs `docker-build` followed
by `docker-push`. `make upload-no-oracle` and `make upload no-oracle` are
retained compatibility forms. At present, `UPLOAD_BUILD_TARGET` is not consumed
by `docker-build`, so these two forms do **not** create a distinct no-Oracle
Docker image.

`make k8deploy` downloads and runs the imagebot client. It requires
`IMAGEBOT_URL`; `TAG`, `IMAGE`, `REPO`, `IMAGEBOT_NAMESPACE`, and
`IMAGEBOT_SERVICE` can be overridden. Repository resolution prefers the
`upstream` remote, then `origin`, then `dmwm/dbs2go`.

`make k8deploy-no-oracle` and `make k8deploy no-oracle` are compatibility aliases
for the same deployment action; deployment itself does not build an image.

### Tests and benchmarks

`make test` runs the main database, SQL, validation, bulk, HTTP, utility,
migration, writer, integration, lexicon, and benchmark suites. On ARM it uses
the Oracle strip/test/restore sequence.

Additional aggregate targets are:

| Target | Function |
| --- | --- |
| `make test_all` | ARM aggregate used by the restoration-aware `test` target. |
| `make test-github` | CI-oriented aggregate including migration request tests. |
| `make test-lexicon` | Run all positive and negative reader/writer lexicon tests. |
| `make test-errors-no-oracle` | Run error tests with Oracle imports temporarily disabled and restored. |
| `make bench` | Run Go benchmarks. |
| `make test-race` | Run the race-condition integration test. |

Individual suites can be run with `test-dbs`, `test-bulk`, `test-sql`,
`test-validator`, `test-http`, `test-writer`, `test-utils`, `test-migrate`,
`test-filelumis`, `test-errors`, `test-integration`, `test-migration`,
`test-migration-requests`, and the four `test-lexicon-*` targets.
