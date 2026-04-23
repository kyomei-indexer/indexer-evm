.PHONY: help build check fmt clippy test run docker release tag-only

BIN_NAME := kyomei-indexer
CARGO_TOML := Cargo.toml
IMAGE ?= ghcr.io/kyomei-indexer/indexer-evm
CONFIG ?= config.yaml

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## cargo build --release
	cargo build --release --locked

check: ## cargo check
	cargo check --locked

fmt: ## cargo fmt
	cargo fmt --all

clippy: ## cargo clippy -D warnings
	cargo clippy --all-targets --locked -- -D warnings

test: ## cargo test
	cargo test --locked

run: ## Run the indexer with $(CONFIG) (default: config.yaml)
	cargo run --release -- --config $(CONFIG)

docker: ## Build the Docker image locally as $(IMAGE):dev
	docker build -t $(IMAGE):dev .

##@ Release

# Usage:
#   make release VERSION=0.2.0
#
# Steps:
#   1. Require a clean working tree on main.
#   2. Ensure CI is green (fmt + clippy + test) locally.
#   3. Bump the version in Cargo.toml and refresh Cargo.lock.
#   4. Commit "release: vX.Y.Z".
#   5. Create annotated tag vX.Y.Z and push both branch and tag.
#
# The tag push triggers .github/workflows/release.yml, which cross-builds
# Linux (x86_64 + aarch64) and macOS (x86_64 + aarch64) binaries, builds
# multi-arch Docker images to ghcr.io, and publishes a GitHub Release
# with tarballs + SHA256 checksums.
release: ## Cut a release: bump version, tag, push. VERSION=X.Y.Z required
ifndef VERSION
	$(error VERSION is required, e.g. `make release VERSION=0.2.0`)
endif
	@echo "==> Preflight: working tree clean?"
	@test -z "$$(git status --porcelain)" || { echo "working tree dirty, commit or stash first"; exit 1; }
	@echo "==> Preflight: on main?"
	@test "$$(git rev-parse --abbrev-ref HEAD)" = "main" || { echo "not on main branch"; exit 1; }
	@echo "==> Preflight: local checks"
	cargo fmt --all -- --check
	cargo clippy --all-targets --locked -- -D warnings
	cargo test --locked
	@echo "==> Bumping $(CARGO_TOML) to version $(VERSION)"
	@sed -i.bak -E 's/^version = "[^"]+"/version = "$(VERSION)"/' $(CARGO_TOML)
	@rm -f $(CARGO_TOML).bak
	@# Refresh Cargo.lock so the package's own version entry matches
	cargo check >/dev/null
	@echo "==> Committing"
	git add $(CARGO_TOML) Cargo.lock
	git commit -m "release: v$(VERSION)"
	@echo "==> Tagging v$(VERSION)"
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	@echo "==> Pushing main and tag"
	git push origin main
	git push origin "v$(VERSION)"
	@echo ""
	@echo "Release v$(VERSION) pushed. Workflow: https://github.com/kyomei-indexer/indexer-evm/actions"

tag-only: ## Tag the current commit without bumping Cargo.toml. VERSION=X.Y.Z required
ifndef VERSION
	$(error VERSION is required, e.g. `make tag-only VERSION=0.2.0`)
endif
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	git push origin "v$(VERSION)"
