from pathlib import Path

import duckdb
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from domains import list_tables
from utilities import forge_name_with_date, get_filepaths, style_panel


def insert_processed_data(
    connection: duckdb.DuckDBPyConnection,
    preprocessing_dir: Path,
    input_file_pattern: str,
    color: str,
):
    """Function to insert parquet file into database's main table."""

    msg = f"""
For each pre-processed parquet file, parse the tweets' publication dates and insert each tweet's data into the table corresponding to the month of the tweet's publication.
    """
    style_panel(msg=msg, color=color, title="Import data")

    connection.execute("PRAGMA disable_progress_bar")

    # Before continuing with this process, remove any existing monthly tables in the database
    all_tables = connection.execute("SHOW TABLES;").fetchall()
    aggregate_tables = list_tables(all_tables, "tweets_from")
    if len(aggregate_tables) > 0:
        for table in aggregate_tables:
            query = f"""
            DROP TABLE {table};
            """
            connection.execute(query)

    # Get a list of all pre-processed parquet files in the pre-processing directory
    parquet_files = get_filepaths(
        data_path=preprocessing_dir, file_pattern=input_file_pattern
    )

    # ----------------------------------------------------------------------- #
    # Set up the progress bar
    ProgressCompleteColumn = Progress(
        TextColumn("{task.description}"),
        MofNCompleteColumn(),
        BarColumn(bar_width=60),
        TimeElapsedColumn(),
        expand=True,
    )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task(f"{color}Parsing date range...", start=False, total=0)
        task2 = progress.add_task(f"{color}Creating tables...", start=False, total=0)
        task3 = progress.add_task(
            f"{color}Importing tweet data...", start=False, total=0
        )
        # ------------------------------------------------------------------ #

        # Start progress bar on task 1: Parsing the dataset's date range
        progress.update(task_id=task1, total=len(parquet_files))
        progress.start_task(task_id=task1)

        # Parse which months are represented in which data files
        months_in_all_files = []
        index_of_files_and_their_months = {}
        for f in parquet_files:
            filepath = str(f)
            query = f"""
            SELECT DISTINCT date_trunc('month', local_time)
            FROM (
                SELECT CAST(local_time AS TIMESTAMP) AS local_time
                FROM read_parquet('{filepath}')
            )
            GROUP BY date_trunc('month', local_time);
            """
            months_in_the_file = [t[0] for t in duckdb.sql(query).fetchall()]
            months_in_all_files.extend(months_in_the_file)
            index_of_files_and_their_months[filepath] = months_in_the_file
            progress.update(task_id=task1, advance=1)
        all_months = set(months_in_all_files)

        # Start progress bar on task 2: Creating tables in the database
        progress.update(task_id=task2, total=(len(all_months)))
        progress.start_task(task_id=task2)

        # Create tables for each month in the dataset
        for month in all_months:
            table_name = forge_name_with_date(prefix="tweets_from", datetime_obj=month)
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
                );
            """
            connection.execute(query)
            progress.update(task_id=task2, advance=1)

        # Start progress bar on task 3: Importing files' data into the database
        progress.update(
            task_id=task3, total=len(index_of_files_and_their_months.keys())
        )
        progress.start_task(task_id=task3)

        # Import tweet data into the table representing the month of the tweet's publication
        for file, months_in_the_file in index_of_files_and_their_months.items():
            for month in months_in_the_file:
                table_name = forge_name_with_date(
                    prefix="tweets_from", datetime_obj=month
                )
                query = f"""
                INSERT INTO {table_name}
                SELECT  md5(domain_name),
                        domain_name,
                        normalized_url,
                        retweeted_id,
                        tweet_id,
                        user_id,
                        local_time,
                FROM (
                    SELECT  id AS tweet_id,
                            CAST(local_time AS TIMESTAMP) AS local_time,
                            user_id,
                            retweeted_id,
                            link,
                            domain AS domain_name,
                            normalized_url
                    FROM read_parquet('{file}')
                )
                WHERE date_trunc('month', local_time) = '{month}';
                """
                connection.execute(query)
            progress.update(task_id=task3, advance=1)
