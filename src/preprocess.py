import datetime
import gzip
import subprocess
import timeit
from pathlib import Path

from CONSTANTS import MAINTABLENAME, TWEET_REDUCED


def add_data(data, con):

    # ---------------------------------------------------------- #
    # Get data files' paths.
    data_path = Path(data)
    if data_path.is_file():
        file_path_objects = [data_path]
    elif data_path.is_dir():
        file_path_objects = [x for x in data_path.iterdir() if x.is_file() if x.suffix == '.csv' or x.suffix == '.gz']
    else:
        raise FileExistsError()

    # ---------------------------------------------------------- #
    # Create the main table for the input tweet data.
    fields = TWEET_REDUCED
    columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
    con.execute(f"CREATE TABLE IF NOT EXISTS {MAINTABLENAME}({columns})")

    # Iterate through the data files and add them to the main table.
    for path_obj in file_path_objects:
        # ---------------------------------------------------------- #
        # Preprocess the file to extract only the necessary columns.
        columns = ','.join([k for k in TWEET_REDUCED.keys()])
        preprocessed_data = 'select_tweet_columns.csv'
        preprocess_script = ['xsv', 'select', '-o', preprocessed_data, columns]
        filepath = path_obj.resolve().__str__()
        with gzip.open(filepath, 'r') as f:
            try:
                f.read(1)
            except:
                shell = False
                script = preprocess_script+[filepath]
            else:
                shell = True
                script = " ".join(['gzcat', filepath, '|']+preprocess_script)
        print(f'\n----------------------------------------------------------\nPreprocessing data from {filepath}')
        print('Began at {}'.format(datetime.datetime.now().time()))
        start = timeit.default_timer()
        subprocess.run(script, shell=shell)
        stop = timeit.default_timer()
        delta = stop - start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

        # ---------------------------------------------------------- #
        print(f'\nImporting data from {preprocessed_data}')
        print('Began at {}'.format(datetime.datetime.now().time()))
        start = timeit.default_timer()
        table = 'input'
        fields = TWEET_REDUCED
        fields.update({'id':'UBIGINT'})
        columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE IF NOT EXISTS {table}({columns})")
        con.execute(f"INSERT INTO {table} SELECT * FROM read_csv('{preprocessed_data}', delim=',', header=True, columns={fields});")
        con.execute(f"COPY {table} TO 'duck/{path_obj.stem}.csv' (HEADER, DELIMITER ',')")
        stop = timeit.default_timer()
        delta = stop - start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

        # ---------------------------------------------------------- #
        print(f'\nMerging imported data from {path_obj.name} into "{MAINTABLENAME}".')
        print('Began at {}'.format(datetime.datetime.now().time()))
        start = timeit.default_timer()
        con.execute(f"""
        INSERT INTO {MAINTABLENAME} 
        SELECT * 
        FROM {table} 
        WHERE {table}.id NOT IN (SELECT id FROM {MAINTABLENAME});
        """)
        con.execute(f"DROP TABLE {table}")
        stop = timeit.default_timer()
        delta = stop - start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))
