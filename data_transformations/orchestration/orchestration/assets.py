import pandas as pd
from dagster import AssetExecutionContext, asset, StaticPartitionsDefinition
from dagster_dbt import DbtCliResource, dbt_assets
from dotenv import load_dotenv

import os

from .constants import dbt_manifest_path


load_dotenv()


@dbt_assets(manifest=dbt_manifest_path)
def data_transformations_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


training_file_partition = StaticPartitionsDefinition([str(i) for i in range(1, int(os.getenv("TRAINING_FILE_NUMBERS")))])


@asset
def training_data():
    file = f"https://raw.githubusercontent.com/hazourahh/big-data-course-2024-projects/master/imdb/train-1.csv"
    pd.read_csv(file, index_col=0).to_parquet(f"data/training[1].parquet")
