RUST_DOCS_TARGET=docs/source/RustDocs

rust-build:
	cargo build -q

rust-build-docs: 
	cargo doc --no-deps --all -q --target-dir $(RUST_DOCS_TARGET)
	rm -rf $(RUST_DOCS_TARGET)/debug
	mv $(RUST_DOCS_TARGET)/doc/* $(RUST_DOCS_TARGET)
	rm -rf $(RUST_DOCS_TARGET)/doc
