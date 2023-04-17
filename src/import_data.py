from pathlib import Path

import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from utilities import get_filepaths, name_table
from domains import list_tables


def insert_processed_data(connection:duckdb, input_dir:Path, input_file_pattern:str):
    """Function to insert parquet file into database's main table."""
    connection.execute('PRAGMA disable_progress_bar')

    # Remove any existing domains table in the database
    all_tables = connection.execute('SHOW TABLES;').fetchall()
    aggregate_tables = list_tables(all_tables, 'domains')
    if len(aggregate_tables) > 0:
        for table in aggregate_tables:
            query = f"""
            DROP TABLE {table};
            """
            connection.execute(query)

    # Get a list of processed parquet files
    parquet_files = get_filepaths(
        data_path=input_dir,
        file_pattern=input_file_pattern
    )
    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task('[bold blue]Parsing date range...', start=False)
        task2 = progress.add_task('[bold blue]Creating tables...', start=False)
        task3 = progress.add_task('[bold blue]Importing tweet data...', start=False)

        # Get a list of all the months in the dataset
        all_months = []
        files_with_months = {}
        progress.update(task_id=task1, total=len(parquet_files))
        progress.start_task(task_id=task1)
        for parquet_filepath_obj in parquet_files:
            filepath_str = str(parquet_filepath_obj)
            query = f"""
            SELECT DISTINCT date_trunc('month', local_time)
            FROM (
                SELECT CAST(local_time AS TIMESTAMP) AS local_time
                FROM read_parquet('{filepath_str}')
            )
            GROUP BY date_trunc('month', local_time);
            """
            months_in_file = [t[0] for t in duckdb.sql(query).fetchall()]
            all_months.extend(months_in_file)
            files_with_months[filepath_str] = months_in_file
            progress.update(task_id=task1, advance=1)
        all_months = set(all_months)

        # Create tables for each month in the dataset
        progress.update(task_id=task2, total=(len(all_months)))
        progress.start_task(task_id=task2)
        for month in all_months:
            table_name = name_table(datetime_obj=month)
            query = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}(
                domain_id VARCHAR,
                domain_name VARCHAR,
                normalized_url VARCHAR,
                retweeted_id VARCHAR,
                tweet_id VARCHAR,
                user_id VARCHAR,
                local_time TIMESTAMP,
                needs_resolved BOOLEAN,
                clean_url VARCHAR,
                );
            """
            connection.execute(query)
            progress.update(task_id=task2, advance=1)

        # Import tweet data to the right month's table
        progress.update(task_id=task3, total=len(files_with_months.keys()))
        progress.start_task(task_id=task3)
        for filepath_str, month_list in files_with_months.items():
            for month in month_list:
                table_name = name_table(datetime_obj=month)
                query = f"""
                INSERT INTO {table_name}
                SELECT  md5(domain_name),
                        domain_name,
                        normalized_url,
                        retweeted_id,
                        tweet_id,
                        user_id,
                        local_time,
                        needs_resolved,
                        clean_url,
                FROM (
                    SELECT  id AS tweet_id,
                            CAST(local_time AS TIMESTAMP) AS local_time,
                            user_id,
                            retweeted_id,
                            link,
                            domain AS domain_name,
                            CAST(needs_resolved AS BOOLEAN) as needs_resolved,
                            clean_url,
                            normalized_url
                    FROM read_parquet('{filepath_str}')
                )
                WHERE date_trunc('month', local_time) = '{month}';
                """
                connection.execute(query)
            progress.update(task_id=task3, advance=1)
