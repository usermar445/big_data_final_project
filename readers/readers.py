from pyspark.sql import SparkSession
from pathlib import Path
from os import path


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
    file_path = data_dir / FileNames.writing_file
    assert path.exists(file_path)
    return str(file_path)


class Data:

    def __init__(self):
        self.spark_connection = (
            SparkSession
            .builder
            .appName("Big_Data_Project")
            .getOrCreate()
        )

        self.train_file_1 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_1))
        self.train_file_2 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_2))
        self.train_file_3 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_3))
        self.train_file_4 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_4))
        self.train_file_5 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_5))
        self.train_file_6 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_6))
        self.train_file_7 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_7))
        self.train_file_8 = self.spark_connection.read.json(get_file_dir(FileNames.train_file_8))
        self.test_hidden_file = self.spark_connection.read.json(get_file_dir(FileNames.test_hidden_file))
        self.directing = self.spark_connection.read.json(get_file_dir(FileNames.directing_file))
        self.writing = self.spark_connection.read.json(get_file_dir(FileNames.writing_file))


if __name__ == '__main__':

    data = Data()
    print(data.train_file_1.show())
