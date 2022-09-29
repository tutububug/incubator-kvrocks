all: prepare build

prepare:
	@cd .. && git submodule update --remote && cd -

build: prepare
	cargo +nightly build

clean:
	cargo clean
