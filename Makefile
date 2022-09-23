ROOT = $(shell git rev-parse --show-toplevel)

CC_DIR := cc

JOINKEYD := $(CC)/joinkey
JOINKEY_GO := $(JOINKEYD)/joinkey.go
JOINKEY_OUTPUT := $(JOINKEYD)/joinkey.output

TARGETD := $(CC)/target
TARGET_GO := $(TARGETD)/target.go
TARGET_OUTPUT := $(TARGETD)/target.output

.PHONY: test
test:
	go test -cover ./...

.PHONY: regenarate
regenarate: clean generate

.PHONY: generate
generate: go-generate $(JOINKEY_GO) $(TARGET_GO)

.PHONY: clean
clean: clean-go-generate clean-join-key clean-target

$(JOINKEY_GO): $(JOINKEYD)/joinkey.y
	goyacc -o $@ -v $(JOINKEY_OUTPUT) $<

.PHONY: clean-join-key
clean-join-key:
	rm -f $(JOINKEY_OUTPUT) $(JOINKEY_GO)

$(TARGET_GO): $(TARGETD)/target.y
	goyacc -o $@ -v $(TARGET_OUTPUT) $<

.PHONY: clean-target
clean-target:
	rm -f $(TARGET_OUTPUT) $(TARGET_GO)

.PHONY: go-regenerate
go-regenerate: clean-go-generate go-generate

.PHONY: clean-go-generate
clean-go-generate:
	find $(ROOT) -name "*_generated.go" -type f | xargs rm -f

.PHONY: go-generate
go-generate:
	go generate ./...
