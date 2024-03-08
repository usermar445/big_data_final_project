include .env
export

TRANSFORMS_FOLDER=data_transformations


format:
	@eval "$$(pyenv init -)" && \
	pyenv shell 3.12 && \
	poetry run black .


ingest-data:
	poetry run python3 -m ingestion.ingest \
		--level $$LOGGING_LEVEL \
		--source_data $$DATA_SOURCE


dbt-run:
	cd $$TRANSFORMS_FOLDER && \
	poetry run dbt run \
	--target $$ENVIRONMENT

dbt-test:
	cd $$TRANSFORMS_FOLDER && \
	poetry run dbt test