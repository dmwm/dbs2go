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
DBS_MIGRATION_SERVERS = dbs2go-global-migration dbs2go-phys03-migration
DBS_SERVER_WAS_SET := $(if $(filter undefined,$(origin DBS_SERVER)),,1)
DBS_SERVER ?= dbs2go-global-r
DBS_STATUS_SERVERS = $(if $(DBS_SERVER_WAS_SET),$(DBS_SERVER),$(DBS_SERVERS))
DBS_SERVER_DEV = $(DBS_SERVER)-dev
DBS_SERVER_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/$(DBS_SERVER).yaml
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
	@echo ">>> Deploying $(DBS_SERVER_DEV) to $(ENV)..."
	@kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER)
ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@kubectl -n $(NAMESPACE) get secret $(DBS_SERVER)-secrets \
		proxy-secrets robot-secrets hmac-secrets token-secrets && \
		kubectl -n $(NAMESPACE) get configmap tnsnames-config
else
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER) && \
		kubectl -n $(NAMESPACE) get secret $(DBS_SERVER)-secrets \
			proxy-secrets robot-secrets hmac-secrets token-secrets && \
		kubectl -n $(NAMESPACE) get configmap tnsnames-config
endif

	# Constrain HPA-managed deployments through their HPA; scale other deployments directly.
ifneq (,$(filter $(DBS_SERVER),$(DBS_HPA_SERVERS)))
	@echo ">>> Constraining hpa/$(DBS_SERVER_HPA) to a single pod:"
	@kubectl -n $(NAMESPACE) patch hpa $(DBS_SERVER_HPA) \
		-p '{"spec":{"minReplicas":1,"maxReplicas":1}}'
else ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@echo ">>> Stopping the regular migration deployment/$(DBS_SERVER):"
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER) --replicas=0
else
	@echo ">>> Scaling deployment/$(DBS_SERVER) to a single pod:"
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER) --replicas=1
endif
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER) --timeout=180s

	@echo ">>> Bringing up $(DBS_SERVER_DEV) empty container..."
	@test -f $(DBS_SERVER_DEV_MANIFEST) || { \
		echo "ERROR: Missing manifest $(DBS_SERVER_DEV_MANIFEST)"; \
		exit 1; \
	}
	@echo ">>> Checking deployment/$(DBS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: deployment/$(DBS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DBS_SERVER_DEV_MANIFEST)
ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER_DEV) --replicas=1
endif
ifeq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@echo ">>> Checking service/$(DBS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: service/$(DBS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DBS_SERVER_DEV_MANIFEST)
endif
	@kubectl -n $(NAMESPACE) wait --for=jsonpath='{.status.phase}'=Running \
		pod -l app=$(DBS_SERVER_DEV) --timeout=180s
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER_DEV)
ifeq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER_DEV)
endif
	@kubectl -n $(NAMESPACE) get pods -l app=$(DBS_SERVER_DEV) -o wide
	@echo ">>> Development pod initialized successfully."

run_dev_push:
	@echo ">>> Pushing locally built $(EXECUTABLE) payload to all $(DBS_SERVER_DEV) pods..."
ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@set -eu; \
	regular_replicas=$$(kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER) -o jsonpath='{.spec.replicas}'); \
	regular_ready=$$(kubectl -n $(NAMESPACE) get deployment $(DBS_SERVER) -o jsonpath='{.status.readyReplicas}'); \
	regular_ready=$${regular_ready:-0}; \
	[ "$$regular_replicas" -eq 0 ] && [ "$$regular_ready" -eq 0 ] || { \
		echo "ERROR: Refusing to start $(DBS_SERVER_DEV) while $(DBS_SERVER) is active ($$regular_ready/$$regular_replicas ready)."; \
		exit 1; \
	}
endif
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
ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@echo ">>> $(DBS_SERVER) is database-triggered; no Service redirect is required."
else
	@echo ">>> Preserving the current $(DBS_SERVER) Service manifest from $(ENV) to $(BACKUP_DIR):"
	@kubectl -n $(NAMESPACE) get service $(DBS_SERVER) -o yaml > \
		$(BACKUP_DIR)/$(DBS_SERVER).$(ENV).$(MAKETIME).yaml
	@echo ">>> Redirecting $(DBS_SERVER) traffic to $(DBS_SERVER_DEV) for $(ENV)..."
	@kubectl -n $(NAMESPACE) patch service $(DBS_SERVER) \
		-p '{"spec":{"selector":{"app":"$(DBS_SERVER_DEV)"}}}'
endif

run_dev_revert:
ifneq (,$(filter $(DBS_SERVER),$(DBS_MIGRATION_SERVERS)))
	@echo ">>> Stopping development migration deployment/$(DBS_SERVER_DEV)..."
	@kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER_DEV) --replicas=0
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER_DEV) --timeout=180s
	@set -eu; \
	replicas=$$(awk -v target="$(DBS_SERVER)" ' \
		$$1 == "name:" && $$2 == target { selected=1 } \
		selected && $$1 == "replicas:" { print $$2; exit } \
		' $(DBS_SERVER_MANIFEST)); \
	echo ">>> Restoring deployment/$(DBS_SERVER) to $$replicas replica(s)..."; \
	kubectl -n $(NAMESPACE) scale deployment/$(DBS_SERVER) --replicas="$$replicas"
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DBS_SERVER)
else
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
endif

run_dev_status:
	@echo ">>> Environment [ $(ENV) ], cluster [ $(CLUSTER) ]"
	@printf '%-29s %-11s %-31s %-31s %-7s %s\n' \
		"SERVICE" "ROUTING" "SELECTOR" "ACTIVE DEPLOYMENT" "READY" "ENDPOINTS"; \
	for server in $(DBS_STATUS_SERVERS); do \
		dev_server="$$server-dev"; \
		case " $(DBS_MIGRATION_SERVERS) " in \
			*" $$server "*) migration=true ;; \
			*) migration=false ;; \
		esac; \
		if $$migration; then \
			selector="-"; endpoint_count="-"; \
			regular_desired=$$(kubectl -n $(NAMESPACE) get deployment "$$server" -o jsonpath='{.spec.replicas}' 2>/dev/null || true); \
			regular_ready=$$(kubectl -n $(NAMESPACE) get deployment "$$server" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true); \
			dev_desired=$$(kubectl -n $(NAMESPACE) get deployment "$$dev_server" -o jsonpath='{.spec.replicas}' 2>/dev/null || true); \
			dev_ready=$$(kubectl -n $(NAMESPACE) get deployment "$$dev_server" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true); \
			regular_desired=$${regular_desired:-0}; regular_ready=$${regular_ready:-0}; \
			dev_desired=$${dev_desired:-0}; dev_ready=$${dev_ready:-0}; dev_processes=0; \
			dev_pods=$$(kubectl -n $(NAMESPACE) get pods -l "app=$$dev_server" \
				-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true); \
			while IFS= read -r pod; do \
				[ -n "$$pod" ] || continue; \
				if kubectl -n $(NAMESPACE) exec "$$pod" -c dev -- pgrep -x $(EXECUTABLE) >/dev/null 2>&1; then \
					dev_processes=$$((dev_processes + 1)); \
				fi; \
			done <<< "$$dev_pods"; \
			if [ "$$regular_ready" -gt 0 ] && [ "$$dev_processes" -gt 0 ]; then \
				routing=CONFLICT; active_deployment=MULTIPLE; ready_status="-"; \
			elif [ "$$dev_processes" -gt 0 ]; then \
				routing=DEV-RUNNING; active_deployment="$$dev_server"; ready_status="$$dev_ready/$$dev_desired"; \
			elif [ "$$regular_ready" -gt 0 ]; then \
				routing=REGULAR; active_deployment="$$server"; ready_status="$$regular_ready/$$regular_desired"; \
			elif [ "$$dev_desired" -gt 0 ]; then \
				routing=DEV-IDLE; active_deployment="$$dev_server"; ready_status="$$dev_ready/$$dev_desired"; \
			else \
				routing=INACTIVE; active_deployment="-"; ready_status="-"; \
			fi; \
		else \
			selector=$$(kubectl -n $(NAMESPACE) get service "$$server" -o jsonpath='{.spec.selector.app}' 2>/dev/null || true); \
			case "$$selector" in \
				"$$dev_server") routing=REDIRECTED ;; \
				"$$server") routing=REGULAR ;; \
				"") routing=UNAVAILABLE ;; \
				*) routing=UNKNOWN ;; \
			esac; \
			desired=$$(kubectl -n $(NAMESPACE) get deployment "$$selector" -o jsonpath='{.spec.replicas}' 2>/dev/null || true); \
			ready=$$(kubectl -n $(NAMESPACE) get deployment "$$selector" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true); \
			if [ -n "$$desired" ]; then \
				ready=$${ready:-0}; ready_status="$$ready/$$desired"; \
			else \
				ready_status="-"; \
			fi; \
			active_deployment="$$selector"; \
			endpoint_ips=$$(kubectl -n $(NAMESPACE) get endpoints "$$server" \
				-o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null || true); \
			set -- $$endpoint_ips; endpoint_count=$$#; \
		fi; \
		printf '%-29s %-11s %-31s %-31s %-7s %s\n' \
			"$$server" "$$routing" "$$selector" "$$active_deployment" "$$ready_status" "$$endpoint_count"; \
	done
