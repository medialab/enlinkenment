import datetime
from pathlib import Path


class FileNaming:
    def __init__(self, output_dir:Path, infile:str, remove_prefix:str=None) -> None:
        self.output_dir = output_dir
        self.infile = Path(infile)
        if remove_prefix:
            prefix_len = len(remove_prefix)+1
            stem = infile.name[prefix_len:]
            self.infile = Path(stem)

    def forge_name(self, prefix:str):
        filename = self.infile
        for _ in range(len(filename.suffixes)):
            filename = Path(filename.stem)
        return prefix+'_'+filename.stem

    def parquet(self, prefix:str):
        extension = '.parquet'
        stem = self.forge_name(prefix)
        name = stem+extension
        return self.output_dir.joinpath(name)

    def csv(self, prefix:str):
        extension = '.csv'
        stem = self.forge_name(prefix)
        name = stem+extension
        return self.output_dir.joinpath(name)


def get_filepaths(data_path, file_pattern):
    # Parse all the files in the datapath
    data_path_obj = Path(data_path)
    if data_path_obj.is_file():
        file_path_objects = [data_path_obj]
    elif data_path_obj.is_dir():
        file_path_objects = list(data_path_obj.glob(file_pattern))
    else:
        raise FileExistsError()
    return file_path_objects


def pair_tables(months):
    odd_month = None
    length = len(months)
    if length % 2 != 0:
        odd_month = months.pop()

    joins = [[x,y] for x,y in zip(months[:-1], months[1:])][0::2]

    if odd_month:
        joins.extend([odd_month])

    return joins


def name_table(datetime_obj:datetime.date):
    return 'tweets_from_'+str(datetime_obj.year)+'_'+str(datetime_obj.month)


def extract_month(table_name:str):
    table_name_parts = table_name.split('_')
    year = table_name_parts[-2]
    month = table_name_parts[-1]
    return year+'_'+month


def create_month_column_names(months:list):
    base_name = 'nb_tweets_in_'
    columns = []
    for month in months:
        column_name = base_name+month
        columns.append(column_name)
    return columns


def fill_out_month_columns(month_columns:list, month:str):
    value = f'COUNT(DISTINCT tweet_id) AS nb_tweets_in_{month}'
    selection = []
    for month_column in month_columns:
        if extract_month(month_column) == month:
            selection.append(value)
        else:
            selection.append('0')
    return ', '.join(selection)


def list_tables(all_tables:list, prefix:str):
    return [
        table[0] for table in all_tables 
        if table[0].startswith(prefix)
    ]
