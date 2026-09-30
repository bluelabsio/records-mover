all: typecheck typecoverage coverageclean test coverage flake8

test-reports:
	mkdir test-reports

coverageclean:
	rm -fr .coverage

typecoverageclean:
	rm -fr .mypy_cache

clean: coverageclean typecoverageclean
	FILES=$$(find . -name \*.pyc); for f in $${FILES}; do rm $$f; done

typecheck:
	mypy --cobertura-xml-report typecover --html-report typecover records_mover
	mypy tests

typecoverage:
	python setup.py mypy_ratchet

citypecoverage: typecoverage
	@echo "Looking for un-checked-in type coverage metrics..."
	@git status --porcelain metrics/mypy_high_water_mark
	@test -z "$$(git status --porcelain metrics/mypy_high_water_mark)"

unit:
	ENV=test pytest --cov=records_mover tests/unit
	mv .coverage .coverage-unit

component:
	ENV=test pytest --cov=records_mover tests/component
	mv .coverage .coverage-component

live:
	# Opt-in; needs RECORDS_MOVER_LIVE=1 plus env config (see tests/integration/live/README.md)
	ENV=test pytest tests/integration/live -rs

test: unit component
	coverage combine .coverage-unit .coverage-component # https://stackoverflow.com/questions/7352319/pytest-combined-coverage
	coverage html --directory=cover
	coverage xml

ciunit:
	ENV=test pytest --cov=records_mover tests/unit
	mv .coverage .coverage-unit

cicomponent:
	ENV=test pytest --cov=records_mover tests/component
	mv .coverage .coverage-component

citest: test-reports ciunit cicomponent
	coverage combine .coverage-unit .coverage-component # https://stackoverflow.com/questions/7352319/pytest-combined-coverage
	coverage html --directory=cover
	coverage xml

coverage:
	python setup.py coverage_ratchet

cicoverage: coverage
	@echo "Looking for un-checked-in unit test coverage metrics..."
	@git status --porcelain metrics/coverage_high_water_mark
	@test -z "$$(git status --porcelain metrics/coverage_high_water_mark)"

flake8:
	flake8 --filename='*.py,*.pyi' records_mover tests types

package:
	python3 -m build
	twine check dist/*

docker:
	docker build -f tests/integration/Dockerfile --progress=plain -t records-mover .
