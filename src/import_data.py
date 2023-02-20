import gzip
import os
import subprocess
from pathlib import Path
from sys import platform
from concurrent.futures import ProcessPoolExecutor
import concurrent
import time

from rich.progress import Progress

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


def run_xsv_command(task_id, filepath:Path):
    with gzip.open(str(filepath), 'r') as f:
        try:
            f.read(1)
        except:
            shell = False
            name = filepath.stem
            output = os.path.join(PREPROCESSDIR, f'{name}.csv')
            script = ['xsv', 'select', '-o', output, xsv_column_names, str(filepath)]
        else:
            shell = True
            name = filepath.stem.split('.')[0]
            output = os.path.join(PREPROCESSDIR, f'{name}.csv')
            if platform == "linux" or platform == "linux2":
                decompress = 'zcat'
            elif platform == "darwin":
                decompress = 'gzcat'
            else:
                print('Trying "zcat" command on compressed data files')
                decompress = 'zcat'
            script = f'{decompress} {str(filepath)} | xsv select -o {output} {xsv_column_names}'
    completed_process = subprocess.run(
            script,
            shell=shell,
            capture_output=True
    )
    if completed_process.check_returncode():
        exit()
    return task_id


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
    with Progress() as progress:
        tasks = {
            progress.add_task(
                f'[green]Running XSV command on {fp}',
                total=len(file_path_objects)
            ):fp
            for fp in file_path_objects
        }
        while not progress.finished:
            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(run_xsv_command, task_id, fp) for task_id, fp in tasks.items()]
                for result in concurrent.futures.as_completed(futures):
                    progress.update(task_id=result.result(), advance=1)

    timer.stop()


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
    file_path_objects = list(Path(PREPROCESSDIR).glob('**/*.csv'))
    for i, path_obj in enumerate(file_path_objects):
        filepath = str(path_obj.resolve())

        timer = Timer(f'Importing data to database, file {i+1} of {len(file_path_objects)}')
        print("Caution: Progress bar is not well adapated to reading the progress of CSV import")
        connection.execute(f"""
        DROP TABLE IF EXISTS input;
        """)
        connection.execute(f"""
        CREATE TABLE input
        AS SELECT DISTINCT *
        FROM read_csv_auto('{filepath}');
        """)
        timer.stop()

        timer = Timer('Merge imported data to main table')
        connection.execute(f"""
        INSERT INTO {MAINTABLENAME}
        SELECT input.*
        FROM input
        LEFT JOIN {MAINTABLENAME}
        ON input.id = {MAINTABLENAME}.id
        WHERE {MAINTABLENAME}.id IS NULL
        """)
        connection.execute(f"""
        DROP TABLE input
        """)
        timer.stop()
