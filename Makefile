########################################################################################################################
#
#
########################################################################################################################
.DEFAULT_GOAL := help

# Execute everything on same shell, so the active of the virtualenv works on the next command
#.ONESHELL:

.PHONY: install

#SHELL=./make-venv

# include *.mk

_venv: _venv/touchfile
	
_venv/touchfile: requirements.txt
	test -d venv || virtualenv venv --python=python3
	. venv/bin/activate && python -m pip install -Ur requirements.txt
	touch venv/touchfile

clean: ## clean
	rm -rf venv

upgrade_dependencies: _venv ## Upgrade the pip dependencies
	pip list --outdated --format=freeze | grep -v '^\-e' | cut -d = -f 1  | xargs -n1 pip install -U
	pip freeze > requirements.txt

# Generates a help message. Borrowed from https://github.com/pydanny/cookiecutter-djangopackage.
help: ## Display this help message
	@echo "Please use \`make <target>' where <target> is one of"
	@perl -nle'print $& if m{^[\.a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-25s\033[0m %s\n", $$1, $$2}'
