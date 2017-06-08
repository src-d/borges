# Package configuration
PROJECT = borges
COMMANDS = cli/borges
CODECOV_TOKEN = fd8d47d9-6c72-409f-99fa-a8ba93631860

DEPENDENCIES = github.com/Masterminds/glide

# Including ci Makefile
MAKEFILE = Makefile.main
CI_REPOSITORY = https://github.com/src-d/ci.git
CI_FOLDER = .ci

$(MAKEFILE):
	@git clone --quiet $(CI_REPOSITORY) $(CI_FOLDER); \
	cp $(CI_FOLDER)/$(MAKEFILE) .;

-include $(MAKEFILE)

NOVENDOR_PACKAGES := $(shell go list ./... | grep -v '/vendor/')

BASE_PATH := $(shell pwd)
VENDOR_PATH := $(BASE_PATH)/vendor
BUILD_PATH := $(BASE_PATH)/build

GLIDE = glide

.PHONY: $(DEPENDENCIES) dependencies test test-coverage

dependencies: $(DEPENDENCIES) $(VENDOR_PATH) $(NOVENDOR_PACKAGES)

$(DEPENDENCIES):
	$(GOGET) $@/...

$(NOVENDOR_PACKAGES):
	$(GOGET) $@

$(VENDOR_PATH):
	$(GLIDE) install

test: dependencies
	$(GOTEST) $(NOVENDOR_PACKAGES)

test-coverage: dependencies
	echo "" > $(COVERAGE_REPORT); \
	for dir in $(NOVENDOR_PACKAGES); do \
		$(GOTEST) $$dir -coverprofile=$(COVERAGE_PROFILE) -covermode=$(COVERAGE_MODE); \
		if [ $$? != 0 ]; then \
			exit 2; \
		fi; \
		if [ -f $(COVERAGE_PROFILE) ]; then \
			cat $(COVERAGE_PROFILE) >> $(COVERAGE_REPORT); \
			rm $(COVERAGE_PROFILE); \
		fi; \
	done;

