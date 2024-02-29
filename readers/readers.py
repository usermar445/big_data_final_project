from pyspark.sql import SparkSession
from pathlib import Path
from os import path
import duckdb


class FileNames:

    train_file_1 = "train-1.csv"
    train_file_2 = "train-2.csv"
    train_file_3 = "train-3.csv"
    train_file_4 = "train-4.csv"
    train_file_5 = "train-5.csv"
    train_file_6 = "train-6.csv"
    train_file_7 = "train-7.csv"
    train_file_8 = "train-8.csv"
    test_hidden_file = "test_hidden.csv"
    directing_file = "directing.json"
    writing_file = "writing.json"


def get_file_dir(file_name: str):
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    file_path = data_dir / file_name
    assert path.exists(file_path)
    return str(file_path)


class SparkData:

    def __init__(self):
        self.spark_connection = (
            SparkSession
            .builder
            .appName("Big_Data_Project")
            .getOrCreate()
        )

        self.train_file_1 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_1), header=True)
        self.train_file_2 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_2), header=True)
        self.train_file_3 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_3), header=True)
        self.train_file_4 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_4), header=True)
        self.train_file_5 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_5), header=True)
        self.train_file_6 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_6), header=True)
        self.train_file_7 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_7), header=True)
        self.train_file_8 = self.spark_connection.read.csv(get_file_dir(FileNames.train_file_8), header=True)
        self.test_hidden_file = self.spark_connection.read.csv(get_file_dir(FileNames.test_hidden_file), header=True)
        self.directing = self.spark_connection.read.json(get_file_dir(FileNames.directing_file))
        self.writing = self.spark_connection.read.json(get_file_dir(FileNames.writing_file))


class DuckData:

    def __init__(self):
        self.conn = duckdb.connect()

        self.train_file_1 = self.conn.read_csv(get_file_dir(FileNames.train_file_1))
        self.train_file_2 = self.conn.read_csv(get_file_dir(FileNames.train_file_2))
        self.train_file_3 = self.conn.read_csv(get_file_dir(FileNames.train_file_3))
        self.train_file_4 = self.conn.read_csv(get_file_dir(FileNames.train_file_4))
        self.train_file_5 = self.conn.read_csv(get_file_dir(FileNames.train_file_5))
        self.train_file_6 = self.conn.read_csv(get_file_dir(FileNames.train_file_6))
        self.train_file_7 = self.conn.read_csv(get_file_dir(FileNames.train_file_7))
        self.train_file_8 = self.conn.read_csv(get_file_dir(FileNames.train_file_8))
        self.test_hidden_file = self.conn.read_csv(get_file_dir(FileNames.test_hidden_file))
        self.directing = self.conn.read_csv(get_file_dir(FileNames.directing_file))
        self.writing = self.conn.read_csv(get_file_dir(FileNames.writing_file))