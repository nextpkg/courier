#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
readonly version=2.15.0
lint=${GOLANGCI_LINT:-golangci-lint}
if [[ "$("$lint" version --short)" != "$version" ]]; then
  echo "golangci-lint $version is required" >&2
  exit 1
fi
"$lint" config verify
modules=(./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...)
# Both passes include tests; integration is analyzed without starting brokers.
"$lint" run "${modules[@]}"
"$lint" run --build-tags integration "${modules[@]}"
