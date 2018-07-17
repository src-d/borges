# Package configuration
PROJECT = borges
COMMANDS = cli/borges
GOFLAGS = -tags norwfs

DOCKER_REGISTRY = quay.io
DOCKER_ORG = srcd

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/jfontan/ci.git
CI_BRANCH ?= improvement/add-godep-ensure
CI_PATH ?= .ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)
