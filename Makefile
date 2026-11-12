GOLANGCI_LINT ?= golangci-lint
GOLANGCI_LINT_VERSION := 2.15.0
MODULES := ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...

.PHONY: lint
lint:
	@test "$$("$(GOLANGCI_LINT)" version --short)" = "$(GOLANGCI_LINT_VERSION)" || \
		{ echo "golangci-lint $(GOLANGCI_LINT_VERSION) is required" >&2; exit 1; }
	"$(GOLANGCI_LINT)" config verify
	# Both passes include tests; integration is analyzed without starting brokers.
	"$(GOLANGCI_LINT)" run $(MODULES)
	"$(GOLANGCI_LINT)" run --build-tags integration $(MODULES)
