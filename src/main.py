import os
from pathlib import Path
import shutil

import click
import duckdb

from CONSTANTS import DEFAULTDATABASE, PREPROCESSDIR
from utils import Timer
from import_data import select_columns, import_data
from parse_urls import parse_urls
from aggregate import domains

@click.command()
@click.argument('datapath')
@click.option('-d', '--database-dir', required=False)
def main(datapath, database_dir):
    timer = Timer()

    # ------------------------------------------------------------------ #
    #                         PREPROCESS DATA

    # Make a directory in which to store pre-processed data
    shutil.rmtree(PREPROCESSDIR, ignore_errors=True)
    os.makedirs(PREPROCESSDIR, exist_ok=True)

    # Isolate relevant columns using Rust package XSV
    select_columns(datapath=datapath)

    # ------------------------------------------------------------------ #
    #                         BUILD DATABASE

    # Set up a database in which to store everything
    if not database_dir or Path(database_dir).is_file():
        os.makedirs('database', exist_ok=True)
        database = DEFAULTDATABASE
    else:
        os.makedirs(database_dir)
        database = os.path.join(database_dir, 'tweet_links.db')

    # Connect to the database
    connection = duckdb.connect(database=database, read_only=False)
    connection.execute('PRAGMA enable_progress_bar')

    # Import the pre-processed data files to the database
    import_data(connection)

    # Extract and parse URLs from the data 
    parse_urls(connection)

    # Aggregate the URLs by domain
    domains(connection)

    timer.stop()


if __name__ == "__main__":
    main()
