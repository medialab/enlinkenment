import os
import shutil

import click
import duckdb

from aggregate_data import aggregate_domains, sum_aggregates
from preprocess_data import insert_processed_data, process_data
from utils import Timer, write_output
from constants import DOMAIN_TABLE


@click.command()
@click.argument('data')
@click.option('-f', '--glob-file-pattern', type=click.types.STRING, default='**/*.gz', show_default=True)
@click.option('-d', '--database-name', type=click.types.STRING, default='twitter_links', show_default=True)
@click.option('-o', '--output-dir', type=click.types.STRING, default='output', show_default=True)
@click.option('--skip-preprocessing', is_flag=True, show_default=False, default=False)
def main(data, glob_file_pattern, database_name, output_dir, skip_preprocessing):
    global_timer = Timer()

    # ------------------------------------------------------------------ #
    #                          BUILD DATABASES

    # Confirm that the database directory exists
    os.makedirs('database', exist_ok=True)
    # Name the database file that goes inside the database directory
    duckdb_database_path = os.path.join('database', f'{database_name}.duckdb')

    # Connect to databases
    duckdb_connection = duckdb.connect(duckdb_database_path, read_only=False)
    # Clear tables
    tables = [
        t[0] for t in
        duckdb_connection.execute(f"""
        SHOW TABLES;
        """).fetchall()
    ]
    for table in tables:
        duckdb_connection.execute(f"DROP TABLE {table};")

    # ------------------------------------------------------------------ #
    #                           PROCESS DATA

    # Make a directory in which to store pre-processed data
    if not skip_preprocessing:
        shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    # Isolate relevant columns using arrow csv parser
    if not skip_preprocessing:
        duckdb_connection.execute('PRAGMA disable_progress_bar')
        process_data(
            data=data,
            file_pattern=glob_file_pattern,
            output_dir=output_dir)

    # ------------------------------------------------------------------ #
    #                          AGGREGATE DATA

    timer = Timer(f'----------------------------------------\nInsert data')
    months = insert_processed_data(
        connection=duckdb_connection,
        output_dir=output_dir
    )
    timer.stop()

    timer = Timer(f'----------------------------------------\nAggregate months')
    aggregate_domains(
        connection=duckdb_connection,
        months=months
    )
    timer.stop()

    timer = Timer(f'----------------------------------------\nSum aggregates')
    sum_aggregates(
        connection=duckdb_connection,
        months=months)
    timer.stop()

    # ------------------------------------------------------------------ #
    #                           WRITE OUTPUT

    write_output(
        connection=duckdb_connection,
        output_dir=output_dir,
        filename='domains.csv',
        table_name=DOMAIN_TABLE
    )

    print("---------------------------------------------")
    global_timer.stop()


if __name__ == "__main__":
    main()
