from pathlib import Path

import casanova
import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from aggregate import sum_aggregated_tables
from preprocessing import configure_pyarrow, select_columns
from utilities import (FileNaming, create_month_column_names, extract_month,
                       fill_out_month_columns, list_tables)
from youtube_tools import (YoutubeChannelNormalizer, YoutubeVideoNormalizer,
                           get_youtube_metadata)

FINAL_YOUTUBE_LINKS_TABLE = 'all_youtube_links'
AGGREGATED_YOUTUBE_LINKS_CSV_NAME = 'aggregated_youtube_links.csv'
YOUTUBE_DATA_CSV_NAME = 'youtube_data.csv'
AGGREGATED_YOUTUBE_CHANNEL_CSV_NAME = 'aggregated_youtube_channels.csv'


def aggregate_youtube_links(connection:duckdb, color:str):
    all_tables = connection.execute('SHOW TABLES;').fetchall()

    aggregate_tables = sorted(list_tables(all_tables=all_tables, prefix='youtube_links'))
    if len(aggregate_tables) > 0:
        for table in aggregate_tables:
            query = f"""
            DROP TABLE {table};
            """
            connection.execute(query)

    month_tables = [
        (
            table[0],
            f'youtube_links_in_{extract_month(table[0])}',
            extract_month(table[0])
        )
        for table in all_tables
        if table[0].startswith('tweets_from')
    ]

    # Extract list of months
    months = [i[2] for i in month_tables]
    month_column_names = create_month_column_names(months)
    month_column_names_and_data_types = ', '.join(
        [f'{column_name} UBIGINT' for column_name in month_column_names]
    )

    # Set up the progress bar
    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task(description=f'{color}Aggregating YouTube links...', start=False)

        for tweet_table, youtube_link_table, month_str in month_tables:

            query = f"""
            DROP TABLE IF EXISTS {youtube_link_table};
            CREATE TABLE {youtube_link_table}(
                normalized_url VARCHAR,
                nb_distinct_links UBIGINT,
                nb_collected_retweets UBIGINT,
                sum_all_tweets UBIGINT,
                nb_accounts_that_shared_link UBIGINT,
                {month_column_names_and_data_types}
                );
            """
            connection.execute(query)


            month_column_string = fill_out_month_columns(month_column_names, month_str)
            progress.update(task_id=task1, total=len(month_tables))
            progress.start_task(task_id=task1)
            query = f"""
            INSERT INTO {youtube_link_table}
            SELECT  normalized_url,
                    COUNT(DISTINCT normalized_url),
                    COUNT(DISTINCT retweeted_id),
                    COUNT(DISTINCT tweet_id),
                    COUNT(DISTINCT user_id),
                    {month_column_string}
            FROM (
                SELECT  retweeted_id,
                        tweet_id,
                        user_id,
                        local_time,
                        normalized_url
                FROM {tweet_table}
                WHERE domain_name = 'youtube.com'
            )
            GROUP BY normalized_url
            """
            connection.execute(query)
            progress.update(task_id=task1, advance=1)

def sum_aggregated_youtube_links(connection:duckdb, color:str):
    sum_aggregated_tables(
        connection=connection,
        targeted_table_prefix='youtube_links_in',
        group_by=['normalized_url'],
        message='Summing aggregates of YouTube links',
        color=color
    )
    query = f"""
    DROP TABLE IF EXISTS {FINAL_YOUTUBE_LINKS_TABLE};
    """
    connection.execute(query)
    final_aggregated_table_name = [i[0] for i in connection.execute('SHOW TABLES;').fetchall() if i[0].startswith('youtube')][0]
    query = f"""
    ALTER TABLE {final_aggregated_table_name}
        RENAME TO {FINAL_YOUTUBE_LINKS_TABLE};
    """
    connection.execute(query)


def write_aggregated_youtube_links(output_dir:Path, config:dict, connection:duckdb, color:str):
    outfile = output_dir.joinpath(YOUTUBE_DATA_CSV_NAME)

    aggregated_youtube_links_csv = output_dir.joinpath(AGGREGATED_YOUTUBE_LINKS_CSV_NAME)
    aggregated_youtube_links_csv = str(aggregated_youtube_links_csv)
    query = f"""
    COPY (SELECT * FROM {FINAL_YOUTUBE_LINKS_TABLE}) TO '{aggregated_youtube_links_csv}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)


def aggregate_youtube_channel_data(output_dir:Path, connection:duckdb):

    # Set up file paths
    aggregated_youtube_links_csv = output_dir.joinpath(AGGREGATED_YOUTUBE_LINKS_CSV_NAME)
    infile = output_dir.joinpath(YOUTUBE_DATA_CSV_NAME)
    name_file = FileNaming(output_dir, infile)
    parquet_file_of_select_columns = name_file.parquet('selected_columns')
    outfile = output_dir.joinpath(AGGREGATED_YOUTUBE_CHANNEL_CSV_NAME)

    # Determine relevant columns
    with open(aggregated_youtube_links_csv) as f:
        reader = casanova.reader(f)
        columns = reader.fieldnames
        channel_columns = ['channel_country', 'channel_description', 'channel_id', 'channel_keywords', 'channel_publishedAt', 'channel_subscriberCount', 'channel_title', 'channel_videoCount', 'channel_viewCount']

    # Stream relevant columns from youtube data file
    # and write to parquet file
    select_columns(
        infile=infile,
        outfile=parquet_file_of_select_columns,
        columns=columns+channel_columns
    )

    # Parse column names and datatypes from table
    # prior to data enrichment
    columns_from_before_getting_youtube_data = duckdb.table(FINAL_YOUTUBE_LINKS_TABLE, connection).columns
    data_types_from_before_getting_youtube_data = duckdb.table(FINAL_YOUTUBE_LINKS_TABLE, connection).dtypes
    old_column_string = ', '.join(
        [f'{c[0]} {c[1]}' for c in list(zip(
            columns_from_before_getting_youtube_data,
            data_types_from_before_getting_youtube_data
        ))]
    )

    # Assign data types to columns post data enrichment
    channel_columns_with_data_types = [
        'channel_country VARCHAR',
        'channel_description VARCHAR',
        'channel_id VARCHAR',
        'channel_keywords VARCHAR',
        'channel_publishedAt VARCHAR',
        'channel_subscriberCount UBIGINT',
        'channel_title VARCHAR',
        'channel_videoCount UBIGINT',
        'channel_viewCount UBIGINT'
    ]
    channel_column_string = ', '.join(channel_columns_with_data_types)

    # Drop from the database any old version of the
    # enriched youtube data table
    query = f"""
    DROP TABLE IF EXISTS enriched_youtube_data;
    """
    connection.execute(query)

    # Create the enriched youtube data table with
    # the desired columns and data types
    query = f"""
    CREATE TABLE enriched_youtube_data({old_column_string}, {channel_column_string});
    """
    connection.execute(query)

    # Insert the parsed parquet file into the
    # enriched youtube data table
    query = f"""
    INSERT INTO enriched_youtube_data
    SELECT *
    FROM read_parquet('{parquet_file_of_select_columns}')
    WHERE channel_id IS NOT NULL;
    """
    connection.execute(query)

    # Drop from the database any old version of the
    # aggregated youtube channel table
    query = f"""
    DROP TABLE IF EXISTS aggregated_youtube_channels;
    """
    connection.execute(query)

    # Get list of columns to sum
    summed_columns = [column for column in duckdb.table('enriched_youtube_data', connection).columns if not column.startswith('channel')]
    summed_columns.remove('normalized_url')
    summed_column_string = [f'{c} UBIGINT' for c in summed_columns]

    # Create the aggregated youtube channel table
    query = f"""
    CREATE TABLE aggregated_youtube_channels(
        nb_normalized_links UBIGINT,
        {', '.join(summed_column_string)},
        channel_country VARCHAR,
        channel_description VARCHAR,
        channel_id VARCHAR,
        channel_keywords VARCHAR,
        channel_publishedAt VARCHAR,
        channel_subscriberCount UBIGINT,
        channel_title VARCHAR,
        channel_videoCount UBIGINT,
        channel_viewCount UBIGINT
    );
    """
    connection.execute(query)

    sum_string = ', '.join([f'SUM({c})' for c in summed_columns])

    # Aggregate
    query = f"""
    INSERT INTO aggregated_youtube_channels
    SELECT
        COUNT(normalized_url) AS nb_normalized_links,
        {sum_string},
        channel_id,
        ANY_VALUE(channel_name) AS channel_name,
        ANY_VALUE(channel_country) AS channel_country,
        ANY_VALUE(channel_keywords) AS channel_keywords,
        ANY_VALUE(channel_publishedAt) AS channel_publishedAt,
        ANY_VALUE(channel_subscriberCount) AS channel_subscriberCount,
        ANY_VALUE(channel_title) AS channel_title,
        ANY_VALUE(channel_videoCount) AS channel_videoCount,
        ANY_VALUE(channel_viewCount) AS channel_viewCount
    FROM
        enriched_youtube_data
    GROUP BY channel_id
    """
    connection.execute(query)

    duckdb.table('aggregated_youtube_channels', connection).write_csv(outfile)
