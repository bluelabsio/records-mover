all: typecheck typecoverage coverageclean test coverage quality

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

quality-flake8:
	make QUALITY_TOOL=flake8 quality

quality-punchlist:
	make QUALITY_TOOL=punchlist quality

quality-mdl:
	make QUALITY_TOOL=mdl quality

# to run a single item, you can do: make QUALITY_TOOL=flake8 quality
quality:
	@quality_gem_version=$$(python -c 'import yaml; print(yaml.safe_load(open(".circleci/config.yml","r"))["quality_gem_version"])'); \
	docker run --rm \
	       -v "$$(pwd):/usr/app"  \
	       -v "$$(pwd)/Rakefile.quality:/usr/quality/Rakefile"  \
	       "apiology/quality:$${quality_gem_version}" ${QUALITY_TOOL}

package:
	python3 -m build
	twine check dist/*

docker:
	docker build -f tests/integration/Dockerfile --progress=plain -t records-mover .
