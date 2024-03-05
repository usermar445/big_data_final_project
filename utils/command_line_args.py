from typing import Literal

from pydantic import BaseModel


class CommandLineArgs(BaseModel):

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
    source_data: str
