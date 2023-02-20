import os
from pathlib import Path

import click
import duckdb

from CONSTANTS import DEFAULTDATABASE
from utils import Timer
from import_data import import_data
from parse_urls import parse_urls
from aggregate import domains

@click.command()
@click.argument('datapath')
@click.option('-d', '--database-dir', required=False)
def main(datapath, database_dir):
    timer = Timer()

    # Set up a database in which to store everything
    if not database_dir or Path(database_dir).is_file():
        os.makedirs('database', exist_ok=True)
        database = DEFAULTDATABASE
    else:
        os.makedirs(database_dir)
        database = os.path.join(database_dir, 'tweet_links.db')
    os.makedirs('output', exist_ok=True)

    # Connect to the database
    connection = duckdb.connect(database=database, read_only=False)

    import_data(con=connection, datapath=datapath)

    parse_urls(connection)

    domains(connection)

    timer.stop()


if __name__ == "__main__":
    main()
