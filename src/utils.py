import datetime
import os
import timeit
from pathlib import Path

import duckdb


class Timer:
    def __init__(self, message=None) -> None:
        self.message = message
        self.start = timeit.default_timer()
        if self.message:
            print(f'\n{self.message}')
        print('Began at {}'.format(datetime.datetime.now().time()))

    def stop(self):
        stop = timeit.default_timer()
        delta = stop - self.start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))


def get_filepaths(data, file_pattern):
    # Parse all the files in the datapath
    datapath = Path(data)
    if datapath.is_file():
        file_path_objects = [datapath]
    elif datapath.is_dir():
        file_path_objects = list(datapath.glob(file_pattern))
    else:
        raise FileExistsError()
    return file_path_objects


def write_output(connection, output_dir, filename, table_name):
    outfile_path = os.path.join(output_dir, filename)
    timer = Timer(f'Writing table "{table_name}" to {outfile_path}')
    duckdb.table(
        table_name=table_name,
        connection=connection
    ).to_csv(
        file_name=outfile_path,
        sep=',',
        header=True,
    )
    timer.stop()


def pair_tables(months):
    odd_month = None
    length = len(months)
    if length % 2 != 0:
        odd_month = months.pop()

    joins = [[x,y] for x,y in zip(months[:-1], months[1:])][0::2]

    if odd_month:
        joins.extend([odd_month])

    return joins


def list_tables(connection):
    return [
        t[0] for t in
        connection.execute(f"""
        SHOW TABLES;
        """).fetchall()
    ]
