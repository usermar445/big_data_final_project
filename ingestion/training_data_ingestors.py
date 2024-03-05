import pandas as pd
import pandera as pa

from readers.readers import File


def get_csv_data_stream(source: str, filename: File, chunk_size: int = 1_000):
    return pd.read_csv(
        f"{source}/{filename.file_name}", index_col=0, chunksize=chunk_size
    )


def get_json_data(source: str, filename: File):
    return pd.read_json(f"{source}/{filename.file_name}")


def validate_data(data: pd.DataFrame, validator: pa.DataFrameModel):
    return validator.validate(data)
