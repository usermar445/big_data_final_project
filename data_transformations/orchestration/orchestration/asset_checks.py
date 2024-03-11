from typing import Mapping

from dagster import AssetCheckResult, AssetChecksDefinition, asset_check


def headers_are_correct(check_blob: Mapping[str, str]) -> AssetChecksDefinition:
    @asset_check(
        name=check_blob["name"],
        asset=check_blob["asset"],
    )
    def _check(context, data):
        actual_cols = set(data.columns)
        expected_cols = check_blob["headers"]
        return AssetCheckResult(passed=len(actual_cols.intersection(expected_cols)) == len(expected_cols))

    return _check


def files_not_empty(check_blob: Mapping[str, str]) -> AssetChecksDefinition:
    @asset_check(
        name=check_blob["name"],
        asset=check_blob["asset"],
    )
    def _check(context, data):
        return AssetCheckResult(passed=not data.empty)

    return _check


file_not_empty_blobs = [
    {
        "name": "files_not_empty",
        "asset": "training_data",
    },
    {
        "name": "files_not_empty",
        "asset": "validation_data",
    },
    {
        "name": "files_not_empty",
        "asset": "testing_data",
    },
    {
        "name": "files_not_empty",
        "asset": "directing_data",
    },
    {
        "name": "files_not_empty",
        "asset": "writing_data",
    },
]

headers_correct_blobs = [
    {
        "name": "headers_correct",
        "asset": "training_data",
        "headers": {
            'tconst',
            'primaryTitle',
            'originalTitle',
            'startYear',
            'endYear',
            'runtimeMinutes',
            'numVotes',
            'label',
        }
    },
    {
        "name": "headers_correct",
        "asset": "validation_data",
        "headers": {
            'tconst',
            'primaryTitle',
            'originalTitle',
            'startYear',
            'endYear',
            'runtimeMinutes',
            'numVotes',
        }
    },
    {
        "name": "headers_correct",
        "asset": "testing_data",
        "headers": {
            'tconst',
            'primaryTitle',
            'originalTitle',
            'startYear',
            'endYear',
            'runtimeMinutes',
            'numVotes',
        }
    },
    {
        "name": "headers_correct",
        "asset": "directing_data",
        "headers": {
            'movie',
            'director',
        }
    },
    {
        "name": "headers_correct",
        "asset": "writing_data",
        "headers": {
            'movie',
            'writer',
        }
    },
]
