clean: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find ./fsql/ -name '*.so' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	rm -f results.xml
	rm -fr htmlcov/
	rm -fr .coverage
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .venv-precommit
	rm -rf .venv-test
	rm -f index.html

static-analysis: ## run mypy, black, flake, etc
	(\
		set -e ; \
		python -m venv .venv-precommit; \
		. .venv-precommit/bin/activate; \
		pip install -U pip; \
		pip install pre-commit; \
		pre-commit install --install-hooks; \
		pre-commit run --from-ref origin/main --to-ref HEAD; \
	)

test-suite: ## install new venv, run tests and coverage
	(\
		set -e ; \
		python -m venv .venv-test; \
		. .venv-test/bin/activate; \
		pip install -U pip; \
		pip install .[test]; \
		coverage run --source fsql -m pytest --junit-xml=results.xml tests/; \
		coverage report -m; \
		coverage html; \
	)

install-edit: clean ## install the package in editable mode
	pip install -e .[test]

## TODO implement
release: clean
	(\
		set -e ; \
		git fetch --all --tags ; \
		export TARGET_VERSION=` PYTHONPATH="." python -c 'import setup; print(setup.compute_version())' `; \
		python -m build; \
		PCKG=`ls dist/ | grep 'whl$$'`; \
		NAME=`cat setup.cfg | grep '^name =' | sed 's/.*=[ ]*\(.*\)/\1/'`; \
		git tag "v$${TARGET_VERSION}"; \
		git push origin "v$${TARGET_VERSION}"; \
	)
