import datetime
from pathlib import Path
from typing import Any


class FileNaming:
    """Class whose methods derive new file paths based on a given file's name."""

    def __init__(self, output_dir: Path, infile: Path, remove_prefix: str = "") -> None:
        self.output_dir = output_dir
        self.infile = infile
        if remove_prefix:
            prefix_len = len(remove_prefix) + 1
            stem = infile.name[prefix_len:]
            self.infile = Path(stem)

    def forge_name(self, prefix: str) -> str:
        filename = self.infile
        for _ in range(len(filename.suffixes)):
            filename = Path(filename.stem)
        return prefix + "_" + filename.stem

    def parquet(self, prefix: str) -> Path:
        extension = ".parquet"
        stem = self.forge_name(prefix)
        name = stem + extension
        return self.output_dir.joinpath(name)

    def csv(self, prefix: str) -> Path:
        extension = ".csv"
        stem = self.forge_name(prefix)
        name = stem + extension
        return self.output_dir.joinpath(name)


def get_filepaths(data_path: Path, file_pattern: str) -> list[Path]:
    """Function to get an array of files according to a given pattern."""
    data_path_obj = Path(data_path)
    if data_path_obj.is_file():
        file_path_objects = [data_path_obj]
    elif data_path_obj.is_dir():
        file_path_objects = list(data_path_obj.glob(file_pattern))
    else:
        raise FileExistsError()
    return file_path_objects


def pair_tables(list_of_tables: list) -> list[Any]:
    """Function to transform a list of tables into a nested list of pairs."""
    remaining_odd_table = None
    length = len(list_of_tables)

    if length % 2 != 0:
        remaining_odd_table = list_of_tables.pop()

    joins = [[x, y] for x, y in zip(list_of_tables[:-1], list_of_tables[1:])][0::2]

    if remaining_odd_table:
        joins.extend([remaining_odd_table])

    return joins


def forge_name_with_date(prefix: str, datetime_obj: datetime.date) -> str:
    """Function to build a string with parsed date information."""
    return prefix + "_" + str(datetime_obj.year) + "_" + str(datetime_obj.month)


def extract_month(table_name: str) -> str:
    """Following the syntax from forge_name_with_date, this function removes everything that is not date information from a string."""
    table_name_parts = table_name.split("_")
    year = table_name_parts[-2]
    month = table_name_parts[-1]
    return year + "_" + month


def create_month_column_names(months: list) -> list:
    """Function to generate a column name for every month in the data."""
    base_name = "nb_tweets_in_"
    columns = []
    for month in months:
        column_name = base_name + month
        columns.append(column_name)
    return columns


def build_aggregate_command_for_month_columns(month_columns: list, month: str):
    """Function to generate the SQL aggregation command for every month in data."""
    value = f"COUNT(DISTINCT tweet_id) AS nb_tweets_in_{month}"
    selection = []
    for month_column in month_columns:
        if extract_month(month_column) == month:
            selection.append(value)
        else:
            selection.append("0")
    return ", ".join(selection)


def list_tables(all_tables: list, prefix: str):
    """Function to generate a simple list of all tables in the array returned with duckdb's list table method."""
    return [table[0] for table in all_tables if table[0].startswith(prefix)]


def log_time_message(step: str, duration: str):
    """Function to document a process's duration."""
    return f"{step} - {duration}"
