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
	mypy --cobertura-xml-report typecover --html-report typecover .
	mypy tests

typecoverage:
	python setup.py mypy_ratchet

citypecoverage: typecoverage
	@echo "Looking for un-checked-in type coverage metrics..."
	@git status --porcelain metrics/mypy_high_water_mark
	@test -z "$$(git status --porcelain metrics/mypy_high_water_mark)"

unit:
	ENV=test nosetests --cover-package=records_mover --cover-erase --with-coverage --with-xunit --cover-html --cover-xml --cover-inclusive tests/unit

component:
	ENV=test nosetests --cover-package=records_mover --with-coverage --with-xunit --cover-html --cover-xml --cover-inclusive tests/component

test: unit component

citest: test-reports
	ENV=test nosetests --cover-package=records_mover --with-coverage --with-xunit --cover-html --cover-xml --cover-inclusive --xunit-file=test-reports/junit.xml tests/unit

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

quality-bigfiles:
	make QUALITY_TOOL=bigfiles quality

quality-mdl:
	make QUALITY_TOOL=mdl quality

# to run a single item, you can do: make QUALITY_TOOL=bigfiles quality
quality:
	@quality_gem_version=$$(python -c 'import yaml; print(yaml.safe_load(open(".circleci/config.yml","r"))["quality_gem_version"])'); \
	docker run  \
	       -v "$$(pwd):/usr/app"  \
	       -v "$$(pwd)/Rakefile.quality:/usr/quality/Rakefile"  \
	       "apiology/quality:$${quality_gem_version}" ${QUALITY_TOOL}

package:
	python3 setup.py sdist bdist_wheel

docker:
	docker build -f tests/integration/Dockerfile --progress=plain -t records-mover .
