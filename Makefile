help:
	@grep '^[^\.#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fix: fmt
	@echo "Running cargo clippy --fix"
	@cargo clippy --fix --all-features --allow-dirty --allow-staged

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt

test:
	cargo test --release


