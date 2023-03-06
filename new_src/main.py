import os
import shutil

import click
import duckdb
from aggregate import domains
from CONSTANTS import DOMAINTABLE
from import_data import parse_raw_data
from utils import Timer


@click.command()
@click.argument('datapath')
@click.option('-f', '--glob-file-pattern', type=click.types.STRING, default='**/*.gz', show_default=True)
@click.option('-d', '--database-name', type=click.types.STRING, default='twitter_links', show_default=True)
@click.option('--save-data', is_flag=True, show_default=False, default=False)
def main(datapath, glob_file_pattern, database_name, save_data):
    timer = Timer()

    # ------------------------------------------------------------------ #
    #                          BUILD DATABASES

    # Confirm that the database directory exists
    os.makedirs('database', exist_ok=True)
    # Name the database file that goes inside the database directory
    duckdb_database_path = os.path.join('database', f'{database_name}.duckdb')

    # Connect to databases
    duckdb_connection = duckdb.connect(duckdb_database_path, read_only=False)

    # ------------------------------------------------------------------ #
    #                           PROCESS DATA

    # Make a directory in which to store pre-processed data
    if not save_data:
        shutil.rmtree('output', ignore_errors=True)
    os.makedirs('output', exist_ok=True)

    # Isolate relevant columns using arrow csv parser
    if not save_data:
        duckdb_connection.execute('PRAGMA disable_progress_bar')
        parse_raw_data(
            datapath=datapath,
            file_pattern=glob_file_pattern,
            duckdb_connection=duckdb_connection)

    # ------------------------------------------------------------------ #
    #                          AGGREGATE DATA

    domains(connection=duckdb_connection)

    # ------------------------------------------------------------------ #
    #                           WRITE OUTPUT

    print('Aggregated domain data')
    outfile_path = os.path.join('output', 'domains.csv')
    timer = Timer(f'Writing CSV to {outfile_path}')
    duckdb.table(
        table_name=DOMAINTABLE,
        connection=duckdb_connection
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
