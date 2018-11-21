# Package configuration
PROJECT = go-billy-gluster

# If you need to build more than one dockerfile, you can do so like this:
# DOCKERFILES = Dockerfile_filename1:repositoryname1 Dockerfile_filename2:repositoryname2 ...

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_BRANCH ?= v1
CI_PATH ?= .ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH);
-include $(MAKEFILE)

container:
	bash -x ./.test_setup.sh
