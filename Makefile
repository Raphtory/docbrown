RUST_READTHEDOCS_DOCS_TARGET=docs/source/_rustdoc

rust-build:
	cargo build -q

rust-build-docs: 
	cargo doc --no-deps --all -q

rust-build-readthedocs: 
	cargo doc --no-deps --all -q --target-dir $(RUST_READTHEDOCS_DOCS_TARGET)
	rm -rf $(RUST_READTHEDOCS_DOCS_TARGET)/debug
	mv $(RUST_READTHEDOCS_DOCS_TARGET)/doc/* $(RUST_READTHEDOCS_DOCS_TARGET)
	rm -rf $(RUST_READTHEDOCS_DOCS_TARGET)/doc

build-all: rust-build
	cd python && maturin develop

rust-test:
	cargo test -q

test-all: rust-test
	cd python && pytest

install-python:
	cd python && env VIRTUAL_ENV=$(python3 -c 'import sys; print(sys.base_prefix)') maturin develop
