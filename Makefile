.PHONY: default build build-dev build-prod build-release build-debug dev prod release debug check check-dev check-prod test clippy fmt clean

# Default target is 'prod' (no debug build unless asked for specifically)
default: prod
build: prod

prod: build-prod
build-prod:
	cargo build --workspace --profile prod

release: build-release
build-release:
	cargo build --workspace --release

dev: build-dev
build-dev:
	cargo build --workspace --profile dev

# Debug build is explicit and not run unless asked
debug: build-debug
build-debug:
	cargo build --workspace

check: check-prod
check-prod:
	cargo check --workspace --profile prod

check-dev:
	cargo check --workspace --profile dev

test:
	cargo test --workspace --profile prod

clippy:
	cargo clippy --workspace --profile prod -- -D warnings

fmt:
	cargo fmt

clean:
	cargo clean
	rm -rf target
