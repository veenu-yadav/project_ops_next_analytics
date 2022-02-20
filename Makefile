include config.mk

.PHONY : clean lint test docker-login docker-push deploy-app delete-app docker-build build-tbio

lint:
	pylint --disable=all --enable=unused-import $(git ls-files '*.py')
    black --diff --check $(git ls-files '*.py')
    isort $(git ls-files '*.py')