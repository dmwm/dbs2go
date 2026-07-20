# dbs2go development-pod pilot

This optional workflow follows the `das2go` development flow while accounting
for DBS Oracle runtime requirements. It tests the current local source in the
real DBS Kubernetes runtime without publishing the locally built development
image. The default pilot service is the read-only `dbs2go-global-r`. Override
it with `DBS_SERVER=<service>` when another pilot manifest becomes available.
The development resource and manifest names are derived as `<service>-dev` and
`kubernetes/cmsweb/services/<service>-dev.yaml`.

## Kubernetes environment

The Make targets do not choose, configure, or switch Kubernetes environments.
Configure the environment before running them. The environment name is detected
with:

```bash
kubectl config get-contexts -o name
```

The intended aliases and the environment names reported by
`kubectl config get-contexts -o name` are:

| Alias | Environment name |
|---|---|
| `testbed` | `cmsweb-testbed-backend` |
| `preprod` | `cmsweb-testbed-backend` |
| `dev` | `cmsweb-test1` |
| `test1` | `cmsweb-test1` |

The Makefile does not receive an environment argument. It requires exactly one
detected context and accepts `cmsweb-testbed-backend` or development test
environments matching `cmsweb-test[0-9]+[0-9]*`. The underlying kubeconfig
cluster name is displayed for information only. Every cluster-changing
top-level target requires interactive confirmation through `/dev/tty`.

For the duration of the CMSKubernetes development work, `setup_config` uses:

```text
https://github.com/todor-ivanov/CMSKubernetes.git
feature_CreateDbsDevEnv
```

The upstream `dmwm/CMSKubernetes` repository and `master` branch remain
commented beside this temporary configuration in `devops.mk` for restoration
after the corresponding changes are merged.

## Development flow

```bash
make -f devops.mk devinit
make -f devops.mk devpush
make -f devops.mk devstatus
make -f devops.mk devrevert
```

`devinit` prepares CMSKubernetes configuration, creates the selected dedicated
development resources, preserves the current Service selector, and redirects
the selected Service to its development pod.

`devpush` runs `make docker build dev`, which compiles the current source through
CMSKubernetes `Dockerfile.dev`. It extracts `dbs2go` and `static` from the local
image, copies the payload into the development pod, and restarts the process.

`devinit` preserves the complete current Service manifest in `tmp/backup.d`
before redirecting. `devrevert` restores the standard `app=<DBS_SERVER>` Service
selector. Always run it after testing. `devstatus` reports the detected
environment, current Service selector, and selected development resources.

To use another service after its corresponding manifest has been provided:

```bash
make -f devops.mk devinit DBS_SERVER=dbs2go-phys03-r
make -f devops.mk devpush DBS_SERVER=dbs2go-phys03-r
make -f devops.mk devrevert DBS_SERVER=dbs2go-phys03-r
```

The DAS-style `deploy`, `push_image`, and `run_deploy` targets remain generic
placeholders for future deployment development. They are separate from the
`devinit`/`devpush`/`devrevert` development loop.
