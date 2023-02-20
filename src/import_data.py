from pathlib import Path
import gzip
from CONSTANTS import MAINTABLENAME
from utils import Timer
import subprocess
from sys import platform
import os

main_table_name = MAINTABLENAME
input_table_name = 'input'
input_columns_dict = {
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
input_columns_string = ', '.join([f'{k} {v}' for k,v in input_columns_dict.items()])
input_columns_headers_no_space = ','.join([k for k in input_columns_dict.keys()])
os.makedirs('temp', exist_ok=True)
select_columns_csv = os.path.join('temp', 'select_tweet_columns.csv')
xsv_script = ['xsv', 'select', '-o', select_columns_csv, input_columns_headers_no_space]


def import_data(con, datapath):
    """Iterate through Tweet data files and import into a main table."""

    # If one doesn't already exist, create a main table 
    # in which to store all imported Tweet data
    timer = Timer('Creating main table')
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {main_table_name}({input_columns_string});
    """)
    timer.stop()

    input_data_path = Path(datapath)
    if input_data_path.is_file():
        file_path_objects = [input_data_path]
    elif input_data_path.is_dir():
        file_path_objects = list(input_data_path.glob('**/*.csv*'))
    else:
        raise FileExistsError()

    # Iterate through the input datasets
    for i, path_obj in enumerate(file_path_objects):
        print(f'Processing file {i+1} of {len(file_path_objects)}')

        # Select reduced Tweet columns with XSV in command line
        timer = Timer(f'Using XSV to select relevant columns: {xsv_script[-1]}')
        with gzip.open(str(path_obj), 'r') as f:
            try:
                f.read(1)
            except:
                shell = False
                script = xsv_script+[str(path_obj)]
            else:
                shell = True
                if platform == "linux" or platform == "linux2":
                    decompress = 'zcat'
                elif platform == "darwin":
                    decompress = 'gzcat'
                else:
                    print('Trying "zcat" command on compressed data files')
                    decompress = 'zcat'
                script = " ".join([decompress, str(path_obj), '|']+xsv_script)
        completed_process = subprocess.run(script, shell=shell, capture_output=True)
        if completed_process.check_returncode():
            exit()
        timer.stop()

        # Import the reduced Tweet columns
        timer = Timer('Importing selected columns to database')
        con.execute(f"""
        DROP TABLE IF EXISTS {input_table_name}
        """)
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {input_table_name}({input_columns_string})
        """)
        con.execute(f"""
        INSERT INTO {input_table_name}
        SELECT DISTINCT *
        FROM read_csv('{select_columns_csv}', delim=',', header=True, columns={input_columns_dict});
        """)
        timer.stop()

        # Merge the imported data into a central table for all Tweet data
        timer = Timer('Merging dataset into main table')
        con.execute(f"""
        INSERT INTO {main_table_name}
        SELECT {input_table_name}.*
        FROM {input_table_name}
        LEFT JOIN {main_table_name}
        ON {input_table_name}.id = {main_table_name}.id
        WHERE {main_table_name}.id IS NULL
        """)
        con.execute(f"DROP TABLE {input_table_name}")
        timer.stop()
