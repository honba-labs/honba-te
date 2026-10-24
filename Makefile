.PHONY: default build build-dev build-prod build-release build-debug dev prod release debug check check-dev check-prod test clippy fmt clean

# Default target is 'dev'
default: dev
build: dev

dev: build-dev
build-dev:
	cargo build --workspace --profile dev

prod: build-prod
build-prod:
	cargo build --workspace --profile prod

release: build-release
build-release:
	cargo build --workspace --release

# Debug build is explicit and not run unless asked
debug: build-debug
build-debug:
	cargo build --workspace

check: check-dev
check-dev:
	cargo check --workspace --profile dev

check-prod:
	cargo check --workspace --profile prod

test:
	cargo test --workspace

clippy:
	cargo clippy --workspace -- -D warnings

fmt:
	cargo fmt

clean:
	cargo clean
	rm -rf target
