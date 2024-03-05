# Big Data Project 

## Project Submission:
- URL: http://big-data-competitions.swedencentral.cloudapp.azure.com:8080/
- Username: group2
- Password: HiC5EfVW

## Project specifics:

The current project is set up to run against `python3 v. 3.12`.
The setup created with `poetry` and all dependencies are listed in the `pyproject.toml` file.

## Working with the Makefile
After ensuring your terminal is running python 3.12 and you have all your poetry dependencies installed, you can run:

```bash
make ingest-data
```
This command will begin the ingestion of all data.

## Convenience functions

The Data class in the readers.readers package will read all files for you.
You can access them by the given attribute as seen in the code snippet below.

```python

from readers.readers import DuckData
import duckdb

data = DuckData()
training_data = data.all_training_data

duckdb.query("FROM training_data")
```