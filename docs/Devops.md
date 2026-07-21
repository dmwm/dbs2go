# dbs2go development-pod pilot

This optional workflow follows the `das2go` development flow while accounting
for DBS Oracle runtime requirements. It tests the current local source in the
real DBS Kubernetes runtime without publishing the locally built development
image. The default pilot service is the read-only `dbs2go-global-r`. Select any
supported pilot with `DBS_SERVER=<service>`.

Supported pilots are:

```text
dbs2go-global-r
dbs2go-global-w
dbs2go-global-m
dbs2go-global-migration
dbs2go-phys03-r
dbs2go-phys03-w
dbs2go-phys03-m
dbs2go-phys03-migration
```
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
make -f devops.mk devscale dbs2go-global-r 5
make -f devops.mk devpush
make -f devops.mk devstatus
make -f devops.mk devrevert
```

`devinit` prepares CMSKubernetes configuration, creates the selected dedicated
development resources, preserves the current Service selector, and redirects
the selected Service to its development pod.

`devscale <count>` manually scales the selected development Deployment to the
requested positive number of pods and waits for its rollout. `devpush` runs the
established `make build-ora` native Oracle build, then copies the resulting
`dbs2go` executable and the local `static` directory into every selected
development pod and restarts the process in each one. It does not rebuild the
development container image. The image referenced by the selected development
manifest must already be available from the registry.

For an HPA-managed pilot, `devinit` constrains its regular HPA to one replica;
otherwise it scales the regular Deployment directly. `devinit` preserves the
selected Service in `tmp/backup.d` before redirecting it. `devrevert` restores
HPA limits from the CMSKubernetes DBS HPA manifest when applicable and restores
the standard `app=<DBS_SERVER>` Service selector. Always run it after testing.
`devstatus` reports the detected environment, current Service selector, and
selected development resources.

The `dbs2go-global-migration` and `dbs2go-phys03-migration` workers are
database-triggered and use a separate switchover path. `devinit` scales the
regular migration Deployment to zero before starting one idle development pod.
`devpush` refuses to start the development executable unless the regular
Deployment is fully stopped. `devrevert` scales the development Deployment to
zero before restoring the regular replica count from its CMSKubernetes
manifest. Migration status reports `REGULAR`, `DEV-RUNNING`, `DEV-IDLE`,
`CONFLICT`, or `INACTIVE` based on regular replicas and development processes.

Example status report:

```console
$ make -f devops.mk devstatus
>>> Environment [ cmsweb-testbed-backend ], cluster [ cmsweb-k8s-prodsrv-v1.22.9 ]
SERVICE                       ROUTING     SELECTOR                        ACTIVE DEPLOYMENT               READY   ENDPOINTS
dbs2go-global-r               REDIRECTED  dbs2go-global-r-dev             dbs2go-global-r-dev             5/5     5
dbs2go-global-w               REGULAR     dbs2go-global-w                 dbs2go-global-w                 1/3     1
dbs2go-global-m               REGULAR     dbs2go-global-m                 dbs2go-global-m                 1/1     1
dbs2go-global-migration       REGULAR     -                               dbs2go-global-migration         1/1     -
dbs2go-phys03-r               REDIRECTED  dbs2go-phys03-r-dev             dbs2go-phys03-r-dev             1/1     1
dbs2go-phys03-w               REGULAR     dbs2go-phys03-w                 dbs2go-phys03-w                 3/3     3
dbs2go-phys03-m               REGULAR     dbs2go-phys03-m                 dbs2go-phys03-m                 1/1     1
dbs2go-phys03-migration       DEV-RUNNING -                               dbs2go-phys03-migration-dev     1/1     -
```

To use another service after its corresponding manifest has been provided:

```bash
make -f devops.mk devinit dbs2go-phys03-r
make -f devops.mk devpush dbs2go-phys03-r
make -f devops.mk devscale dbs2go-phys03-r 3
make -f devops.mk devrevert dbs2go-phys03-r
```

The existing variable form remains supported, for example
`make -f devops.mk devpush DBS_SERVER=dbs2go-phys03-r`. With that form,
`devscale` remains `make -f devops.mk devscale 3
DBS_SERVER=dbs2go-phys03-r`.

The DAS-style `deploy`, `push_image`, and `run_deploy` targets remain generic
placeholders for future deployment development. They are separate from the
`devinit`/`devpush`/`devrevert` development loop.
