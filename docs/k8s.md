## DBS deployment on k8s cluster
DBS service is composed by multiple instances:
- [DBS reader](https://github.com/dmwm/dbs2go/blob/master/docs/DBSReader.md)
  is a service dedicated to read APIs
- [DBS writer](https://github.com/dmwm/dbs2go/blob/master/docs/DBSWriter.md)
  is a service dedicated to write APIs
- [DBS migration](https://github.com/dmwm/dbs2go/blob/master/docs/MigrationServer.md)
  service is composed by two instances:
  - DBS migrate service which exposes public APIs
  - DBS migration service is the internal migration daemon to perform migration
    tasks
Moreover, DBS connects to different DBS back-ends, like production
or testbed or development databases. Thereore, on k8s we use the following
convention to identify DBS service:
```
dbs-<instance>-<kind>
```
where instance, can be `global` for DBS production global instance,
or `phys03` for DBS physics03 instance (in a past we had multiple
physics instances). The `kind` suffix has the following values:
`r` to identify reader instance, `w` for writer intstance, and
`m` for migrate service and `migration` for migration one.

Therefore, in practice, we'll have the following instances on k8s cluster:
```
dbs2go-global-m
dbs2go-global-migration
dbs2go-global-r
dbs2go-global-w
dbs2go-phys03-m
dbs2go-phys03-migration
dbs2go-phys03-r
dbs2go-phys03-w
```
where we deploy multiple instance per service, i.e. five `dbs-global-r`
instances, etc.

### CMSWEB
To deploy DBS on CMSWEB k8s cluster we need few things in place:
- deployment script from [CMSKubernetes](https://github.com/dmwm/CMSKubernetes) repository
- DBS secret files
- working k8s cluster

To proceed, please follow these steps:
1. login to lxplus8
2. clone [CMSKubernetes](https://github.com/dmwm/CMSKubernetes) repository
   - please note, all k8s related stuff can be found in `kubernetes`
     sub-directory
3. clone [services_config](https://gitlab.cern.ch/cmsweb-k8s/services_config)
   repository
   - please note, we keep multiple branches for specific deployment, therefore
   you must use concrete branch to get your configuration files, e.g. `prod`
   for production files, `preprod` for preproduction testbed cluster, and
   `test` for development `test-X` clusters.
   - list of appropriate CMSWEB cluster can be found
   [here](https://cms-http-group.docs.cern.ch/k8s_cluster/cmsweb_developers_k8s_documentation/)
4. clone [k8s_admin_config](https://gitlab.cern.ch/cmsweb-k8s-admin/k8s_admin_config)
   repository
5. setup your favorite k8s cluster environment
```
export KUBECONFIG=/path/k8s_admin_config/config.test10/config.cmsweb-test3
```
6. deploy DBS secrets
```
cd CMSKubernetes/kubernetes/scripts
deploy-secrets.sh <namespace> <service-name> <path_to_configuration>
# for example
deploy-secrets.sh dbs dbs-global-r /path/services_config/dbs2go-global-r
```
7. deploy concrete DBS service
```
deploy-srv.sh <service> <tag> <env>
# for example, to deploy dbs2go-global-r service in test environment (test3) we will use
deploy-srv.sh dbs2go-global-r v0.0.0 test3
```
