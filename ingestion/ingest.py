import sys
from typing import Type

from loguru import logger
import fire

from ingestion.training_data_ingestors import get_csv_data_stream, get_json_data
from ingestion.validation import (
    TrainingValidation,
    DirectorsValidation,
    WritersValidation,
    TestValidation,
)
from utils.command_line_args import CommandLineArgs
from readers.readers import FileNames, File


csv_validation_type = Type[TrainingValidation] | Type[TestValidation]
json_validation_type = Type[DirectorsValidation] | Type[WritersValidation]


def pull_csv_data_to_local(
    source: str, file: File, logger, validator: csv_validation_type
):

    logger.info(f"Reading file: {file.file_name}")
    try:
        data_stream = get_csv_data_stream(source=source, filename=file, chunk_size=100)
        file.as_parquet().remove_file_if_exists()

        for i, df in enumerate(data_stream):
            logger.info(f"Validating File: {file.file_name}")
            df = validator.validate(df)

            if not file.file_exists():
                file_name = file.absolute_file_path
                logger.info(f"Creating File {file_name}.")
                df.to_parquet(file_name, engine="fastparquet")
            else:
                file_name = file.absolute_file_path
                logger.info(f"Appending to File: {file_name}")
                df.to_parquet(file_name, engine="fastparquet", append=True)
    #
    except Exception as e:
        logger.error(str(e))
        logger.exception(e)


def pull_json_data_to_local(
    source: str, file: File, logger, validator: json_validation_type
):

    logger.info(f"Reading file: {file.file_name}")
    try:
        df = get_json_data(source, file)
        logger.info(f"Validating File: {file.file_name}")
        df = validator.validate(df)
        file.as_parquet().remove_file_if_exists()
        file_name = file.absolute_file_path
        logger.info(f"Creating File {file_name}.")
        df.to_parquet(file_name, engine="fastparquet")

    except Exception as e:
        logger.error(str(e))
        logger.exception(e)


def main(command_line_args: CommandLineArgs):

    logger.add(
        sys.stdout, format="{time} {level} {message}", level=command_line_args.level
    )

    pull_json_data_to_local(
        source=command_line_args.source_data,
        file=FileNames.directing_file,
        logger=logger,
        validator=DirectorsValidation,
    )

    pull_json_data_to_local(
        source=command_line_args.source_data,
        file=FileNames.writing_file,
        logger=logger,
        validator=WritersValidation,
    )

    pull_csv_data_to_local(
        source=command_line_args.source_data,
        file=FileNames.test_hidden_file,
        logger=logger,
        validator=TestValidation,
    )

    for file in FileNames.csv_data_sets():
        pull_csv_data_to_local(
            source=command_line_args.source_data,
            file=file,
            logger=logger,
            validator=TrainingValidation,
        )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(CommandLineArgs(**kwargs)))
