import os
import shutil

import click
import duckdb

from aggregate import domains, tweets_per_month
from CONSTANTS import JOINEDDOMAINFREQUENCYTABLE
from import_data import parse_raw_data
from utils import Timer


@click.command()
@click.argument('datapath')
@click.option('-f', '--glob-file-pattern', type=click.types.STRING, default='**/*.gz', show_default=True)
@click.option('-d', '--database-name', type=click.types.STRING, default='twitter_links.db', show_default=True)
@click.option('--save-data', is_flag=True, show_default=False, default=False)
def main(datapath, glob_file_pattern, database_name, save_data):
    timer = Timer()

    # ------------------------------------------------------------------ #
    #                          BUILD DATABASE

    # Confirm that the database directory exists
    os.makedirs('database', exist_ok=True)
    # Name the database file that goes inside the database directory
    database_path = os.path.join('database', database_name)

    # Connect to the database
    connection = duckdb.connect(database_path, read_only=False)

    # ------------------------------------------------------------------ #
    #                           PROCESS DATA

    # Make a directory in which to store pre-processed data
    if not save_data:
        shutil.rmtree('output', ignore_errors=True)
    os.makedirs('output', exist_ok=True)

    # Isolate relevant columns using arrow csv parser
    if not save_data:
        connection.execute('PRAGMA disable_progress_bar')
        parse_raw_data(
            datapath=datapath,
            file_pattern=glob_file_pattern,
            connection=connection)

    # ------------------------------------------------------------------ #
    #                          AGGREGATE DATA

    connection.execute('PRAGMA enable_progress_bar')
    # connection.execute(f"""
    # CREATE INDEX tweet_link_idx ON {MAINTABLENAME} (tweet_id, link);
    # """)

    domains(connection=connection)
    tweets_per_month(connection=connection)

    # ------------------------------------------------------------------ #
    #                           WRITE OUTPUT

    print('Aggregated domain data')
    print(duckdb.table(JOINEDDOMAINFREQUENCYTABLE, connection=connection).describe())

    outfile_path = os.path.join('output', 'domains.csv')
    timer = Timer(f'Writing CSV to {outfile_path}')
    duckdb.table(
        table_name=JOINEDDOMAINFREQUENCYTABLE,
        connection=connection
    ).to_csv(
        file_name=outfile_path,
        sep=',',
        header=True,
    )
    timer.stop()

    print("---------------------------------------------")
    timer.stop()


if __name__ == "__main__":
    main()
