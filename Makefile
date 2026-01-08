help:
	@grep '^[^\.#[:space:]].*:' Makefile

check-license-tool:
	@command -v dd-rust-license-tool > /dev/null 2>&1 || { echo "dd-rust-license-tool not found. Install using 'cargo install dd-rust-license-tool'"; exit 1; }

license-check: check-license-tool
	dd-rust-license-tool --manifest-path chitchat/Cargo.toml check

license-fix: check-license-tool
	dd-rust-license-tool --manifest-path chitchat/Cargo.toml write > LICENSE-3rdparty.csv

fix: fmt
	@echo "Running cargo clippy --fix"
	@cargo clippy --fix --all-features --allow-dirty --allow-staged

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt

test:
	cargo test --release


