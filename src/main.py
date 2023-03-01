import os
from pathlib import Path
import shutil

import click
import duckdb

from utils import Timer
from import_data import select_columns, import_data
from parse_urls import parse_urls, aggregating_links
from aggregate import domains

@click.command()
@click.argument('datapath')
@click.option('-d', '--database-dir', required=False)
@click.option('--save-data', is_flag=True, show_default=False, default=False)
@click.option('--save-database', is_flag=True, show_default=False, default=False)
def main(datapath, database_dir, save_data, save_database):
    timer = Timer()

    # ------------------------------------------------------------------ #
    #                         PREPROCESS DATA

    # Make a directory in which to store pre-processed data
    if not save_data:
        shutil.rmtree('output', ignore_errors=True)
    os.makedirs('output', exist_ok=True)

    # Isolate relevant columns using arrow csv parser
    select_columns(datapath=datapath)

    # ------------------------------------------------------------------ #
    #                         BUILD DATABASE

    # Get the name of the database directory
    if not database_dir or Path(database_dir).is_file():
        database_dir = 'database'
    # If not saving prior relations, remove the database directory if it exists
    if not save_database:
        shutil.rmtree(database_dir, ignore_errors=True)
    # Otherwise, confirm that the database directory exists
    else:
        os.makedirs(database_dir, exist_ok=True)
    # Name the database file that goes inside the database directory
    database = os.path.join(database_dir, 'twitter_links.db')

    # Connect to the database
    connection = duckdb.connect(database=database, read_only=False)
    connection.execute('PRAGMA enable_progress_bar')

    # Import the pre-processed data files to the database
    print("\n---------------------------------------------")
    print("-----------------DATA IMPORT-----------------")
    import_data(connection)

    # Extract and parse URLs from the data
    print("\n---------------------------------------------")
    print("-----------------PARSE URLS------------------")
    parse_urls(connection)

    print("\n---------------------------------------------")
    print("---------------AGGREGATE LINKS---------------")
    aggregating_links(connection)

    # Aggregate the URLs by domain
    print("\n---------------------------------------------")
    print("--------------AGGREGATE DOMAINS--------------")
    domains(connection)

    print("---------------------------------------------")
    timer.stop()


if __name__ == "__main__":
    main()
