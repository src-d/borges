# Package configuration
PROJECT = borges
COMMANDS = cli/borges
GOFLAGS = -tags norwfs

DOCKER_REGISTRY = quay.io
DOCKER_ORG = srcd

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= $(shell pwd)/.ci

MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)
