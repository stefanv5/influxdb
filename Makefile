export PKG_CONFIG=./pkg-config.sh

.PHONY: build docs-check

build:
	go build ./...

docs-check:
	python docs/specs/v1.8.10-vs-v1.12.3/check_docs.py
