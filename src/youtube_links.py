import itertools
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path

import casanova
import duckdb
import rich.progress
from minet.youtube.scrapers import scrape_channel_id
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)
import concurrent.futures
from ural.youtube import YoutubeChannel, YoutubeVideo, is_youtube_url, parse_youtube_url

from aggregate import AggregateSQL
from exceptions import MissingTable
from utilities import list_tables


def youtube_link_aggregate_sql() -> AggregateSQL:
    new_table_columns = [
        "normalized_url VARCHAR",
        "nb_collected_retweets_with_links UBIGINT",
        "sum_all_tweets_with_link UBIGINT",
        "nb_accounts_that_shared_link UBIGINT",
    ]
    select = """
            normalized_url,
            COUNT(DISTINCT retweeted_id),
            COUNT(DISTINCT tweet_id),
            COUNT(DISTINCT user_id),
    """
    return AggregateSQL(
        new_table_constant_columns=new_table_columns,
        select=select,
        where="domain_name = 'youtube.com'",
        group_by="normalized_url",
    )


def export_youtube_links(connection: duckdb.DuckDBPyConnection, outfile: str):
    """Function to clean up after aggregation of YouTube links and to export result."""

    # If more than 1 table exists with the prefix "domains", the recursive aggregation of target tables failed
    all_tables = connection.execute("SHOW TABLES;").fetchall()
    domain_tables = sorted(list_tables(all_tables=all_tables, prefix="youtube"))
    if not len(domain_tables) == 1:
        raise MissingTable
    sole_remaining_domain_table = domain_tables[0]

    # Create a table for the finalized domain data with a generated column that counts original tweets
    columns = duckdb.table(sole_remaining_domain_table, connection).columns
    data_types = duckdb.table(sole_remaining_domain_table, connection).dtypes
    columns_and_data_types = [f"{i[0]} {i[1]}" for i in list(zip(columns, data_types))]
    query = f"""
    DROP TABLE IF EXISTS all_youtube_links;
    CREATE TABLE all_youtube_links(
        {', '.join(columns_and_data_types)},
        nb_collected_original_tweets UBIGINT AS (sum_all_tweets_with_link - nb_collected_retweets_with_links) VIRTUAL
    );
    """
    connection.execute(query)

    # Insert the summed, aggregated domain data into the new table with its generated column
    query = f"""
    INSERT INTO all_youtube_links
    SELECT {', '.join(columns)}
    FROM {sole_remaining_domain_table}
    ORDER BY sum_all_tweets_with_link DESC;
    """
    connection.execute(query)

    # Having copied its contents to the final domain table, drop the old result of the recursive aggregation of previous domain tables
    query = f"""
    DROP TABLE {sole_remaining_domain_table};
    """
    connection.execute(query)

    # Export the final domain table to an out-file
    query = f"""
    COPY (SELECT * FROM all_youtube_links) TO '{outfile}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)


def parse_links(infile: Path, outfile: Path, color: str):
    """Function to get channel ID of all YouTube links."""
    count = casanova.reader.count(str(infile))
    with open(infile, "r") as f, open(outfile, "w") as of:
        enricher = casanova.enricher(f, of, add=["channel_id"])
        ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
        )
        with ProgressCompleteColumn as progress:
            task_id = progress.add_task(
                description=f"{color}Parsing YouTube links...", total=count
            )
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for row, normalized_url in enricher.cells(
                    column="normalized_url", with_rows=True
                ):
                    args = [row, normalized_url, progress, task_id]
                    future = executor.submit(parsing_worker, *args)
                    row, add = future.result()
                    enricher.writerow(row, add)


def parsing_worker(
    row: list,
    normalized_url: str,
    progress: Progress,
    task_id: rich.progress.TaskID,
):
    channel_id = ""
    if is_youtube_url(normalized_url):
        parsed_url = parse_youtube_url(normalized_url)
        if isinstance(parsed_url, YoutubeChannel):
            channel_id = parsed_url.id
        elif isinstance(parsed_url, YoutubeVideo):
            url_with_protocol = "https://" + str(normalized_url)
            channel_id = scrape_channel_id(url_with_protocol)
    progress.update(task_id=task_id, advance=1)
    return row, [channel_id]
