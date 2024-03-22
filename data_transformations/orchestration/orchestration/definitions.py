import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .asset_checks import *
from .assets import *
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[
        data_transformations_dbt_assets,
        training_data,
        validation_data,
        testing_data,
        directing_data,
        writing_data,
        test
    ],
    asset_checks=[
        headers_are_correct(check_blob) for check_blob in headers_correct_blobs
    ]
    + [files_not_empty(check_blob) for check_blob in file_not_empty_blobs],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)
