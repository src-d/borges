# Package configuration
PROJECT = borges
COMMANDS = cli/borges

GO_BUILD_ENV = CGO_ENABLED=0
GO_TAGS = norwfs

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_BRANCH ?= v1
CI_PATH ?= .ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)
