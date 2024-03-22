import unicodedata
from typing import Mapping, Any, Optional

import duckdb
import pandas as pd
from dagster import (
    AssetExecutionContext,
    asset,
    StaticPartitionsDefinition,
)
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model,
)
from dotenv import load_dotenv

import os

from utils.command_line_args import get_env_data
from .constants import dbt_manifest_path, dbt_project_dir
from .data_schema import *


load_dotenv()


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "transform"


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def data_transformations_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
):
    yield from dbt.cli(["build"], context=context).stream()


training_file_partition = StaticPartitionsDefinition(
    [
        str(i)
        for i in range(
            int(os.getenv("TRAINING_FILE_START_RANGE")),
            int(os.getenv("TRAINING_FILE_END_RANGE")) + 1,
        )
    ]
)


@asset(group_name="extract", partitions_def=training_file_partition, compute_kind="pandas")
def training_data(context: AssetExecutionContext):
    env_data = get_env_data()
    partition_str = context.partition_key
    file = f"{env_data.source_data}/train-{partition_str}.csv"
    df = pd.read_csv(file, index_col=0)
    TrainingValidation.validate(df)
    df.primaryTitle = (
        df
        .primaryTitle
        .apply(lambda x:
                       unicodedata.normalize('NFKD', x)
               .encode('ascii', 'ignore')
               .decode('ascii')
               )
    )
    df.to_parquet(f"data/train-{partition_str}.parquet")
    return df


@asset(group_name="extract", compute_kind="pandas")
def validation_data():
    env_data = get_env_data()
    file = f"{env_data.source_data}/{env_data.validation_file_name}.csv"
    df = pd.read_csv(file, index_col=0)
    TestValidation.validate(df)
    df.primaryTitle = (
        df
        .primaryTitle
        .apply(lambda x:
                       unicodedata.normalize('NFKD', x)
               .encode('ascii', 'ignore')
               .decode('ascii')
               )
    )
    df.to_parquet(f"data/{env_data.validation_file_name}.parquet")
    return df


@asset(group_name="extract", compute_kind="pandas")
def testing_data():
    env_data = get_env_data()
    file = f"{env_data.source_data}/{env_data.test_file_name}.csv"
    df = pd.read_csv(file, index_col=0)
    TestValidation.validate(df)
    df.primaryTitle = (
        df
        .primaryTitle
        .apply(lambda x:
                       unicodedata.normalize('NFKD', x)
               .encode('ascii', 'ignore')
               .decode('ascii')
               )
    )
    df.to_parquet(f"data/{env_data.test_file_name}.parquet")
    return df


@asset(group_name="extract", compute_kind="pandas")
def directing_data(context: AssetExecutionContext):
    env_data = get_env_data()
    file = f"{env_data.source_data}/{env_data.directing_file_name}.json"
    df = pd.read_json(file)
    DirectorsValidation.validate(df)
    df.to_parquet(f"data/{env_data.directing_file_name}.parquet")
    return df


@asset(group_name="extract", compute_kind="pandas")
def writing_data():
    env_data = get_env_data()
    file = f"{env_data.source_data}/{env_data.writing_file_name}.json"
    df = pd.read_json(file)
    WritersValidation.validate(df)
    df.to_parquet(f"data/{env_data.writing_file_name}.parquet")
    return df


@asset(
    group_name="extract",
    compute_kind="python",
    deps=get_asset_key_for_model([data_transformations_dbt_assets], "combined_training_data")
)
def test(context: AssetExecutionContext):
    connection = duckdb.connect(os.fspath(dbt_project_dir.joinpath("dbt.duckdb")))
    data = connection.sql("select * from combined_training_data").df()
    context.log.info(data.head())
    data.to_parquet("potato.parquet")