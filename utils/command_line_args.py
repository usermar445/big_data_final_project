import os
from typing import Literal

from pydantic import BaseModel


class CommandLineArgs(BaseModel):

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
    environment: Literal["dev", "prod"]
    source_data: str
    training_file_start_range: int
    training_file_end_range: int
    test_file_name: str
    validation_file_name: str
    directing_file_name: str
    writing_file_name: str


def get_env_data():
    return CommandLineArgs(
        level=os.environ.get("LOGGING_LEVEL"),
        environment=os.environ.get("ENVIRONMENT"),
        source_data=os.environ.get("DATA_SOURCE"),
        training_file_start_range=int(os.environ.get("TRAINING_FILE_START_RANGE")),
        training_file_end_range=int(os.environ.get("TRAINING_FILE_END_RANGE")),
        validation_file_name=os.environ.get("VALIDATION_FILE_NAME"),
        test_file_name=os.environ.get("TESTING_FILE_NAME"),
        directing_file_name=os.environ.get("DIRECTING_FILE_NAME"),
        writing_file_name=os.environ.get("WRITING_FILE_NAME"),
    )