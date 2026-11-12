GOLANGCI_LINT ?= golangci-lint
GOLANGCI_LINT_VERSION := 2.15.0
MODULES := ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
.DEFAULT_GOAL := check

.PHONY: check check-tool fmt fmt-check lint install-hooks
check: fmt-check lint

check-tool:
	@test "$$("$(GOLANGCI_LINT)" version --short)" = "$(GOLANGCI_LINT_VERSION)" || \
		{ echo "golangci-lint $(GOLANGCI_LINT_VERSION) is required" >&2; exit 1; }
	"$(GOLANGCI_LINT)" config verify

fmt: check-tool
	"$(GOLANGCI_LINT)" fmt $(MODULES)

fmt-check: check-tool
	"$(GOLANGCI_LINT)" fmt --diff $(MODULES) || \
		{ echo 'Run make fmt and stage the changes.' >&2; exit 1; }

lint: check-tool
	# Both passes include tests; integration is analyzed without starting brokers.
	"$(GOLANGCI_LINT)" run $(MODULES)
	"$(GOLANGCI_LINT)" run --build-tags integration $(MODULES)

install-hooks:
	@hook=$$(git rev-parse --git-path hooks/pre-commit); \
		if test -e "$$hook"; then echo "Existing hook left untouched: $$hook" >&2; exit 1; fi; \
		mkdir -p "$$(dirname "$$hook")" && \
		printf '#!/bin/sh\nexec make check\n' > "$$hook" && chmod +x "$$hook"
