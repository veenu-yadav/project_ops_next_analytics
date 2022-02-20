include config.mk

lint:
		black --diff --check $(git ls-files '*.py')
		isort $(git ls-files '*.py')
		isort $(git ls-files '*.py')
