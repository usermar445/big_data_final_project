# Big Data Project 

## Project Submission:
- URL: http://big-data-competitions.swedencentral.cloudapp.azure.com:8080/
- Username: group2
- Password: HiC5EfVW

## Project specifics:

The current project is set up to run against `python3 v. 3.12`.
The setup created with poetry and all dependencies are listed in the `pyproject.toml` file.

Since PySpark relies on Java, we need to install it. Go to https://www.java.com/en/ for the package and install it.

## Working with the RDDs

The Data class in the readers.readers package will read all files for you.

You can access them by the given attribute as seen in the code snippet below.

```python

from readers.readers import Data


data = Data()

data.train_file_1.show()
data.train_file_2.show()
data.train_file_3.show()
data.train_file_4.show()
data.train_file_5.show()
data.train_file_6.show()
data.train_file_7.show()
data.train_file_8.show()
data.test_hidden_file.show()
data.directing.show()
data.writing.show()

```