test: libsql-test packages-test
.PHONY: test

libsql-test:
	@cargo build
	@./testing/run
.PHONY: libsq-test

packages-test:
	@cd packages/js/libsql-client/ && yarn install && yarn build && yarn test
.PHONY: packages-test
