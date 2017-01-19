# Package configuration
PROJECT = borges 
COMMANDS = cli/borges
CODECOV_TOKEN = 232aa99b-da28-45dd-af33-0be97208ec76

# Including devops Makefile
MAKEFILE = Makefile.main
DEVOPS_REPOSITORY = https://github.com/src-d/devops.git
DEVOPS_FOLDER = .devops
CI_FOLDER = .ci

$(MAKEFILE):
	@git clone --quiet $(DEVOPS_REPOSITORY) $(DEVOPS_FOLDER); \
	cp -r $(DEVOPS_FOLDER)/ci .ci; \
	rm -rf $(DEVOPS_FOLDER); \
	cp $(CI_FOLDER)/$(MAKEFILE) .;

-include $(MAKEFILE)
