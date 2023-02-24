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
@click.option('-o', '--output-dir', required=False)
@click.option("--save", is_flag=True, show_default=False, default=False)
def main(datapath, output_dir, save):
    timer = Timer()

    # ------------------------------------------------------------------ #
    #                         PREPROCESS DATA

    # Make a directory in which to store pre-processed data
    if not save:
        shutil.rmtree('output', ignore_errors=True)
    os.makedirs('output', exist_ok=True)

    # Isolate relevant columns using arrow csv parser
    select_columns(datapath=datapath)

    # ------------------------------------------------------------------ #
    #                         BUILD DATABASE

    # Set up a database in which to store everything
    if not output_dir or Path(output_dir).is_file():
        output_dir = 'output'
        os.makedirs('output', exist_ok=True)
    else:
        os.makedirs(output_dir, exist_ok=True)
    database = os.path.join(output_dir, 'twitter_links.db')

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
