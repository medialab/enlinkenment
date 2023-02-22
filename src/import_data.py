import os
from pathlib import Path

import pyarrow
import pyarrow.csv
import pyarrow.parquet
from rich.progress import (Progress, SpinnerColumn, TextColumn,
                           TimeElapsedColumn)

from CONSTANTS import MAINTABLENAME, PREPROCESSDIR
from utils import Timer

# tweet column data fields
tweet_columns_dict = {
    'id':'UBIGINT',
    'timestamp_utc':'UBIGINT',
    'local_time': 'TIME',
    'retweet_count':'INTEGER',
    'like_count':'INTEGER',
    'reply_count':'INTEGER',
    'user_id':'VARCHAR',
    'user_followers':'INTEGER',
    'user_friends':'INTEGER',
    'retweeted_id':'VARCHAR',
    'retweeted_user_id':'VARCHAR',
    'quoted_id':'VARCHAR',
    'quoted_user_id':'VARCHAR',
    'links':'VARCHAR',
}

# tweet column data fields as a string
input_columns_string = ', '.join(
    [f'{k} {v}' for k,v in tweet_columns_dict.items()]
)

# tweet column data fields for xsv command
xsv_column_names = ','.join(
    [k for k in tweet_columns_dict.keys()]
)


def select_columns(datapath):
    """Iterate through Tweet data files and import into a main table."""

    input_data_path = Path(datapath)
    if input_data_path.is_file():
        file_path_objects = [input_data_path]
    elif input_data_path.is_dir():
        file_path_objects = list(input_data_path.glob('**/*.csv*'))
    else:
        raise FileExistsError()

    # Pre-process the input datasets
    timer = Timer('Pre-processing data by extracting relevant columns')
    with Progress(
        TextColumn('[progress.description]{task.description}'),
        SpinnerColumn(),
        TimeElapsedColumn()
    ) as progress_bars:
        tasks = {
            progress_bars.add_task(
                f'[green]Streaming... {fp.name}',
                total=len(file_path_objects),
                start=False
            ):fp
            for fp in file_path_objects
        }
        for task_id, fp in tasks.items():
            progress_bars.start_task(task_id=task_id)
            csv_to_parquet(in_path=fp)
            progress_bars.stop_task(task_id=task_id)
    timer.stop()


def csv_to_parquet(in_path):
    name = in_path.stem.split('.')[0]
    out_path = os.path.join(PREPROCESSDIR, f'{name}.parquet')
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = [key for key in tweet_columns_dict.keys()]
    parser_options = pyarrow.csv.ParseOptions()
    parser_options.newlines_in_values = True
    writer = None
    with pyarrow.csv.open_csv(str(in_path), convert_options=convert_options, parse_options=parser_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pyarrow.parquet.ParquetWriter(out_path, next_chunk.schema)
            next_table = pyarrow.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()



def import_data(connection):

    # If one doesn't already exist, create a main table 
    # in which to store all imported Tweet data
    timer = Timer('Creating main table')
    connection.execute(f"""
    CREATE TABLE IF NOT EXISTS {MAINTABLENAME}({input_columns_string});
    """)
    timer.stop()

    # Iteratively import pre-processed data
    # Note: looping is necessary to avoid memory problem
    file_path_objects = list(Path(PREPROCESSDIR).glob('**/*.parquet'))
    for i, path_obj in enumerate(file_path_objects):
        filepath = str(path_obj.resolve())

        timer = Timer(f'Importing data to database, file {i+1} of {len(file_path_objects)}')
        connection.execute(f"""
        DROP TABLE IF EXISTS input;
        """)
        connection.execute(f"""
        CREATE TABLE input
        AS SELECT *
        FROM read_parquet('{filepath}')
        WHERE links IS NOT NULL;
        """)
        timer.stop()

        timer = Timer('Merge imported data to main table')
        connection.execute(f"""
        INSERT INTO {MAINTABLENAME}
        SELECT DISTINCT input.*
        FROM input
        LEFT JOIN {MAINTABLENAME}
        ON input.id = {MAINTABLENAME}.id
        WHERE {MAINTABLENAME}.id IS NULL
        """)
        connection.execute(f"""
        DROP TABLE input
        """)
        timer.stop()
