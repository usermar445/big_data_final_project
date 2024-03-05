import os
from pathlib import Path
from os import path
import duckdb


class File:

    def __init__(self, file_name):
        self._file_name = file_name
        self.extension = ".csv"

    @property
    def absolute_file_path(self):
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        file_path = data_dir / self.file_name
        return str(file_path)

    @property
    def file_name(self):
        return self._file_name + self.extension

    def file_exists(self):
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        file_path = data_dir / self.file_name
        return path.exists(file_path)

    def remove_file_if_exists(self):
        if self.file_exists():
            os.remove(self.absolute_file_path)

    def as_csv(self):
        self.extension = ".csv"
        return self

    def as_json(self):
        self.extension = ".json"
        return self

    def as_pickle(self):
        self.extension = ".pickle"
        return self

    def as_parquet(self):
        self.extension = ".parquet"
        return self


class FileNames:

    train_file_1 = File("train-1").as_csv()
    train_file_2 = File("train-2").as_csv()
    train_file_3 = File("train-3").as_csv()
    train_file_4 = File("train-4").as_csv()
    train_file_5 = File("train-5").as_csv()
    train_file_6 = File("train-6").as_csv()
    train_file_7 = File("train-7").as_csv()
    train_file_8 = File("train-8").as_csv()
    test_hidden_file = File("test_hidden").as_csv()
    directing_file = File("directing").as_json()
    writing_file = File("writing").as_json()

    @classmethod
    def csv_data_sets(cls):
        return [
            cls.train_file_1,
            cls.train_file_2,
            cls.train_file_3,
            cls.train_file_4,
            cls.train_file_5,
            cls.train_file_6,
            cls.train_file_7,
            cls.train_file_8,
        ]


class DuckData:

    def __init__(self):
        self.conn = duckdb.connect()

        self.train_file_1 = self.conn.read_parquet(
            FileNames.train_file_1.as_parquet().absolute_file_path
        )
        self.train_file_2 = self.conn.read_parquet(
            FileNames.train_file_2.as_parquet().absolute_file_path
        )
        self.train_file_3 = self.conn.read_parquet(
            FileNames.train_file_3.as_parquet().absolute_file_path
        )
        self.train_file_4 = self.conn.read_parquet(
            FileNames.train_file_4.as_parquet().absolute_file_path
        )
        self.train_file_5 = self.conn.read_parquet(
            FileNames.train_file_5.as_parquet().absolute_file_path
        )
        self.train_file_6 = self.conn.read_parquet(
            FileNames.train_file_6.as_parquet().absolute_file_path
        )
        self.train_file_7 = self.conn.read_parquet(
            FileNames.train_file_7.as_parquet().absolute_file_path
        )
        self.train_file_8 = self.conn.read_parquet(
            FileNames.train_file_8.as_parquet().absolute_file_path
        )

        self.testing_file = self.conn.read_parquet(
            FileNames.test_hidden_file.as_parquet().absolute_file_path
        )

        self.directing = self.conn.read_parquet(
            FileNames.directing_file.as_parquet().absolute_file_path
        )
        self.writing = self.conn.read_parquet(
            FileNames.writing_file.as_parquet().absolute_file_path
        )

        self.all_training_data = (
            self.train_file_1
            .union(self.train_file_2)
            .union(self.train_file_3)
            .union(self.train_file_4)
            .union(self.train_file_5)
            .union(self.train_file_6)
            .union(self.train_file_7)
            .union(self.train_file_8)
            .join(self.writing, condition="tconst == movie", how="left")
            .join(self.directing, condition="movie", how="left")
        )