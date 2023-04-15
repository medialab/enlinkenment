import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)
import casanova
from pathlib import Path
from utilities import extract_month, create_month_column_names, fill_out_month_columns, list_tables, pair_tables
from youtube_tools import get_youtube_metadata
from aggregate import sum_aggregated_tables


def aggregate_youtube_links(connection:duckdb):
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

    for tweet_table, youtube_link_table, month_str in month_tables:

        query = f"""
        DROP TABLE IF EXISTS {youtube_link_table};
        CREATE TABLE {youtube_link_table}(
            clean_url VARCHAR,
            nb_distinct_links_from_domain UBIGINT,
            nb_collected_retweets_with_domain UBIGINT,
            sum_all_tweets_with_domain UBIGINT,
            nb_accounts_that_shared_domain_link UBIGINT,
            {month_column_names_and_data_types}
            );
        """
        connection.execute(query)


        month_column_string = fill_out_month_columns(month_column_names, month_str)
        query = f"""
        INSERT INTO {youtube_link_table}
        SELECT  clean_url,
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
                    needs_resolved,
                    clean_url,
                    normalized_url
            FROM {tweet_table}
            WHERE domain_name = 'youtube.com'
        )
        GROUP BY clean_url
        """
        connection.execute(query)

def sum_aggregated_youtube_links(connection:duckdb):
    sum_aggregated_tables(
        connection=connection,
        targeted_table_prefix='youtube_links_in',
        group_by=['clean_url'],
        message='Summing aggregates of YouTube links'
    )


def request_youtube_channel_data(output_dir:Path, key:str, connection:duckdb):
    config = {'youtube':{'key':key}}
    outfile = output_dir.joinpath('youtube_channels.csv')

    all_tables = connection.execute('SHOW TABLES;').fetchall()
    final_youtube_links_tables = list_tables(all_tables=all_tables, prefix='youtube_links')
    if len(final_youtube_links_tables) != 1:
        raise RuntimeError
    final_youtube_aggregated_table = final_youtube_links_tables[0]

    aggregated_youtube_links_csv = output_dir.joinpath('aggregated_youtube_links.csv')
    aggregated_youtube_links_csv = str(aggregated_youtube_links_csv)
    query = f"""
    COPY (SELECT * FROM {final_youtube_aggregated_table}) TO '{aggregated_youtube_links_csv}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)

    with open(aggregated_youtube_links_csv) as f, open(outfile, 'w') as of:
        enricher = casanova.enricher(f, of, add=['id', 'country', 'description', 'keywords', 'title', 'publishedAt', 'subscriberCount', 'videoCount', 'viewCount'])
        for row, url in enricher.cells('clean_url', with_rows=True):
            normalized_data = get_youtube_metadata(url, config)
            if normalized_data:
                supplement = normalized_data.as_row()
                enricher.writerow(row, supplement)
