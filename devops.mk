# Define variables:
# 1. Force Make to use bash instead of the default standard sh
SHELL := /bin/bash
EXECUTABLE := dbs2go
ENV := $(shell kubectl config get-contexts -o name 2>/dev/null)
CLUSTER := $(shell kubectl config view --minify -o jsonpath='{.clusters[0].name}' 2>/dev/null)
MAKETIME := $(shell date +%Y%m%d-%H%M%S)
DBS2GO_SRC := $(shell pwd)

# Configuration variables:
TMP_DIR = $(DBS2GO_SRC)/tmp
# CONFIG_REPO = https://github.com/dmwm/CMSKubernetes.git
# CONFIG_BRANCH = master
CONFIG_REPO = https://github.com/todor-ivanov/CMSKubernetes.git
CONFIG_BRANCH = feature_CreateDbsDevEnv
CONFIG_DIR = $(TMP_DIR)/CMSKubernetes

# Pilot service variables:
NAMESPACE = dbs
DBS_SERVERS = dbs2go-global-r dbs2go-global-w dbs2go-global-m \
	dbs2go-global-migration dbs2go-phys03-r dbs2go-phys03-w \
	dbs2go-phys03-m dbs2go-phys03-migration
DBS_HPA_SERVERS = dbs2go-global-r dbs2go-global-w dbs2go-phys03-r dbs2go-phys03-w
DBS_SERVER_WAS_SET := $(if $(filter undefined,$(origin DBS_SERVER)),,1)
DBS_SERVER ?= dbs2go-global-r
DBS_SERVER_DEV = $(DBS_SERVER)-dev
DBS_SERVER_DEV_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/$(DBS_SERVER_DEV).yaml
DBS_SERVER_HPA = $(DBS_SERVER)-hpa
DBS_HPA_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/hpa/dbs-hpa.yaml

# If the target is `devscale`, consider the second argument the desired replica count.
ifeq (devscale,$(firstword $(MAKECMDGOALS)))
  DEV_REPLICAS := $(word 2,$(MAKECMDGOALS))
  ifneq (,$(DEV_REPLICAS))
    $(eval $(DEV_REPLICAS):;@true)
  endif
endif

# Local backup state:
BACKUP_DIR = $(TMP_DIR)/backup.d

# Setting up all needed ops directories
_dummy := $(shell mkdir -p $(TMP_DIR) $(BACKUP_DIR))

.PHONY: deploy clean build push_image run_deploy confirm_deploy setup_config \
	devinit devpush devscale devrevert devstatus run_dev_init run_dev_push run_dev_scale \
	run_dev_redirect run_dev_revert run_dev_status

# Confirmation step: require interactive confirmation based on the detected environment.
confirm_deploy:
	@[ "$(filter $(DBS_SERVER),$(DBS_SERVERS))" = "$(DBS_SERVER)" ] || { \
		echo "ERROR: Unsupported DBS pilot service [ $(DBS_SERVER) ]."; \
		echo "Allowed services: $(DBS_SERVERS)"; \
		exit 1; \
	}
	@echo "========================================================================"
	@echo " WARNING: You are deploying at K8 environment: [ $(ENV) ]"
	@echo " Kubernetes cluster: [ $(CLUSTER) ]"
	@echo " DBS pilot service: [ $(DBS_SERVER) ]"
	@echo "========================================================================"
	@if [ -z "$(ENV)" ]; then \
		echo "ERROR: Could not detect a pre-configured Kubernetes environment."; \
		exit 1; \
	fi
	@if [ "$$(printf '%s\n' "$(ENV)" | sed '/^$$/d' | wc -l)" -ne 1 ]; then \
		echo "ERROR: Expected exactly one configured Kubernetes context, found: [ $(ENV) ]"; \
		exit 1; \
	fi
	@{ [ "$(ENV)" = "cmsweb-testbed-backend" ] || \
		[[ "$(ENV)" =~ ^cmsweb-test[0-9]+[0-9]*$$ ]]; } || { \
		echo "ERROR: Environment [ $(ENV) ] is not allowed for this development workflow."; \
		exit 1; \
	}
	@printf "Are you sure you want to proceed? [y/N]: " && read ans < /dev/tty; \
	if [ "$$ans" != "y" ] && [ "$$ans" != "Y" ]; then \
		echo "Deployment aborted by user."; \
		exit 1; \
	fi

# Config setup step: ensure tmp/ exists, clone or update the configuration repo.
setup_config:
	@echo ">>> Preparing temporary config space..."
	@mkdir -p $(TMP_DIR)
	@if [ ! -d "$(CONFIG_DIR)/.git" ]; then \
		echo ">>> Cloning deployment repository and tracking branch [ $(CONFIG_BRANCH) ]..."; \
		git clone --branch $(CONFIG_BRANCH) $(CONFIG_REPO) $(CONFIG_DIR); \
	else \
		echo ">>> Repository exists. Fetching updates and switching to branch [ $(CONFIG_BRANCH) ]..."; \
		cd $(CONFIG_DIR) && \
		git fetch origin && \
		git checkout $(CONFIG_BRANCH) && \
		git pull origin $(CONFIG_BRANCH); \
	fi

# Default DevOps flow.
deploy: confirm_deploy clean build push_image run_deploy

devinit: confirm_deploy setup_config run_dev_init run_dev_redirect

devpush: confirm_deploy build run_dev_push

devscale: confirm_deploy run_dev_scale

devrevert: confirm_deploy setup_config run_dev_revert

devstatus: run_dev_status

# 1. Force a regular clean using the standard Makefile.
clean:
	$(MAKE) -f Makefile clean

# 2. Build the current source locally with the prepared Oracle environment.
build:
	@echo ">>> Building $(EXECUTABLE) locally with Oracle support..."
	$(MAKE) -f Makefile build-ora

# 3. Package and push placeholder retained for future deployment development.
push_image:
	@echo ">>> TODO: Packaging and pushing image for $(ENV)..."

# 4. Deployment placeholder retained for future deployment development.
run_deploy:
	@echo ">>> TODO: Deploying $(EXECUTABLE) to $(ENV)..."

run_dev_init:
	@echo ">>> Deploying $(DBS_SERVER_DEV) to $(ENV)..." && \
		kubectl -n $(NAMESPACE) get service $(DBS_SERVER) && \
		kubectl -n $(NAMESPACE) get secret $(DBS_SERVER)-secrets \
			proxy-secrets robot-secrets hmac-secrets token-secrets && \
		kubectl -n $(NAMESPACE) get configmap tnsnames-config

	# Constrain HPA-managed deployments through their HPA; scale other deployments directly.
ifneq (,$(filter $(DBS_SERVER),$(DBS_HPA_SERVERS)))
	@echo ">>> Constraining hpa/$(DBS_SERVER_HPA) to a single pod:"
	@kubectl -n $(NAMESPACE) patch hpa $(DBS_SERVER_HPA) \
		-p '{"spec":{"minReplicas":1,"maxReplicas":1}}'
else
	@echo ">>> Scaling deployment/$(DBS_SERVER) to a single pod:"
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER) --replicas=1
endif
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER)

	@echo ">>> Bringing up $(DBS_SERVER_DEV) empty container..."
	@test -f $(DBS_SERVER_DEV_MANIFEST) || { \
		echo "ERROR: Missing manifest $(DBS_SERVER_DEV_MANIFEST)"; \
		exit 1; \
	}
	@echo ">>> Checking deployment/$(DBS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: deployment/$(DBS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DBS_SERVER_DEV_MANIFEST)
	@echo ">>> Checking service/$(DBS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: service/$(DBS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DBS_SERVER_DEV_MANIFEST)
	@kubectl -n $(NAMESPACE) wait --for=jsonpath='{.status.phase}'=Running \
		pod -l app=$(DBS_SERVER_DEV) --timeout=180s
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get pods -l app=$(DBS_SERVER_DEV) -o wide
	@echo ">>> Development pod initialized successfully."

run_dev_push:
	@echo ">>> Pushing locally built $(EXECUTABLE) payload to all $(DBS_SERVER_DEV) pods..."
	@set -eu; \
	pods=$$(kubectl -n $(NAMESPACE) get pods -l app=$(DBS_SERVER_DEV) \
		-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); \
	[ -n "$$pods" ] || { echo "ERROR: Development pods were not found. Run devinit first."; exit 1; }; \
	while IFS= read -r pod; do \
		echo ">>> Updating $$pod..."; \
		kubectl -n $(NAMESPACE) cp ./dbs2go "$$pod:/data/dbs2go" -c dev; \
		kubectl -n $(NAMESPACE) cp ./static "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) exec "$$pod" -c dev -- chmod +x /data/dbs2go; \
		echo ">>> Restarting $(EXECUTABLE) at pod $$pod..."; \
		kubectl -n $(NAMESPACE) exec "$$pod" -c dev -- sh -c "cd /data/ && \
			echo exec: $(EXECUTABLE) -config /etc/secrets/dbsconfig.json && \
			{ pkill -e $(EXECUTABLE) || true; } && \
			exec /data/dbs2go -config /etc/secrets/dbsconfig.json < /dev/null > /dev/null 2>&1 &"; \
	done <<< "$$pods"

run_dev_scale:
	@[ "$(words $(MAKECMDGOALS))" -eq 2 ] && [[ "$(DEV_REPLICAS)" =~ ^[1-9][0-9]*$$ ]] || { \
		echo "ERROR: Usage: make -f devops.mk devscale <positive-replica-count>"; \
		exit 1; \
	}
	@echo ">>> Scaling deployment/$(DBS_SERVER_DEV) to $(DEV_REPLICAS) pods..."
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER_DEV) --replicas=$(DEV_REPLICAS)
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get pods -l app=$(DBS_SERVER_DEV) -o wide

run_dev_redirect:
	@echo ">>> Preserving the current $(DBS_SERVER) Service manifest from $(ENV) to $(BACKUP_DIR):"
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER) -o yaml > \
		$(BACKUP_DIR)/$(DBS_SERVER).$(ENV).$(MAKETIME).yaml
	@echo ">>> Redirecting $(DBS_SERVER) traffic to $(DBS_SERVER_DEV) for $(ENV)..."
	@kubectl -n $(NAMESPACE) patch service $(DBS_SERVER) \
		-p '{"spec":{"selector":{"app":"$(DBS_SERVER_DEV)"}}}'

run_dev_revert:
ifneq (,$(filter $(DBS_SERVER),$(DBS_HPA_SERVERS)))
	@echo ">>> Restoring hpa/$(DBS_SERVER_HPA) from $(DBS_HPA_MANIFEST)..."
	@set -eu; \
	limits=$$(awk -v target="$(DBS_SERVER_HPA)" ' \
		$$1 == "name:" && $$2 == target { selected=1 } \
		selected && $$1 == "minReplicas:" { min_replicas=$$2 } \
		selected && $$1 == "maxReplicas:" { print min_replicas, $$2; exit } \
		' $(DBS_HPA_MANIFEST)); \
	read -r min_replicas max_replicas <<< "$$limits"; \
	echo ">>> Restoring hpa/$(DBS_SERVER_HPA) replica limits to $$min_replicas/$$max_replicas..."; \
	kubectl -n $(NAMESPACE) patch hpa $(DBS_SERVER_HPA) \
		-p "{\"spec\":{\"minReplicas\":$$min_replicas,\"maxReplicas\":$$max_replicas}}"
endif
	@echo ">>> Reverting $(DBS_SERVER) traffic for $(ENV):"
	@kubectl -n $(NAMESPACE) patch service $(DBS_SERVER) \
		-p '{"spec":{"selector":{"app":"$(DBS_SERVER)"}}}'

run_dev_status:
	@echo ">>> Environment [ $(ENV) ], cluster [ $(CLUSTER) ]"
ifeq ($(DBS_SERVER_WAS_SET),1)
	@echo ">>> $(DBS_SERVER) Service selector:"
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER) -o jsonpath='{.spec.selector}'; echo
	@kubectl -n $(NAMESPACE) get deployment,pod -l app=$(DBS_SERVER_DEV) -o wide
	@kubectl -n $(NAMESPACE) get service,endpoints $(DBS_SERVER_DEV) -o wide
else
	@for server in $(DBS_SERVERS); do \
		dev_server="$$server-dev"; \
		echo "========================================================================"; \
		echo ">>> $$server Service selector:"; \
		kubectl -n $(NAMESPACE) get service "$$server" -o jsonpath='{.spec.selector}' || true; \
		echo; \
		kubectl -n $(NAMESPACE) get deployment,pod -l "app=$$dev_server" -o wide || true; \
		kubectl -n $(NAMESPACE) get service,endpoints "$$dev_server" -o wide || true; \
	done
endif
