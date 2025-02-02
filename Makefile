install: requirements.txt
	pip install --upgrade pip &&\
	pip install -r requirements.txt

setup: 
	# python setup.py install
	pip install . 

lint:
	pylint --disable=R,C *.py &&\
	pylint --disable=R,C ingest_app/*.py &&\
	pylint --disable=R,C ingest_app/*/*.py &&\
	pylint --disable=R,C ingest_app/tests/*.py

test:
	python -m pytest -vv --cov=ingest_app ingest_app/tests

format:
	black *.py &&\
	black ingest_app/*.py &&\
	black ingest_app/*/*.py &&\
	black ingest_app/tests/*.py

all:
	install setup lint format test