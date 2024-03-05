include .env
export

switch-version:



format:
	@eval "$$(pyenv init -)" && \
	pyenv shell 3.12 && \
	poetry run black .


ingest-data:
	poetry run python3 -m ingestion.ingest \
		--level $$LOGGING_LEVEL \
		--source_data $$DATA_SOURCE