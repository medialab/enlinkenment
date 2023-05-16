from pathlib import Path
import csv

import rich.progress
import casanova
import duckdb
from ural.youtube import YoutubeChannel, YoutubeVideo, parse_youtube_url

from aggregate import AggregateSQL
from exceptions import MissingTable
from utilities import list_tables


def youtube_link_aggregate_sql() -> AggregateSQL:
    new_table_columns = [
        "normalized_url VARCHAR",
        "link_for_scraping VARCHAR",
        "nb_collected_retweets_with_links UBIGINT",
        "sum_all_tweets_with_link UBIGINT",
        "nb_accounts_that_shared_link UBIGINT",
    ]
    select = """
            normalized_url,
            ANY_VALUE(link),
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


def parse_youtube_links(infile: Path, channel_outfile: Path, video_outfile: Path):
    total = casanova.reader.count(infile)
    with open(infile) as f, open(channel_outfile, "w") as cof, open(
        video_outfile, "w"
    ) as vof:
        reader = casanova.reader(f)
        v_fieldnames = reader.fieldnames
        video_writer = csv.writer(vof)
        video_writer.writerow(v_fieldnames)
        channel_writer = csv.writer(cof)
        channel_writer.writerow(v_fieldnames + ["channel_id"])
        for row, url in rich.progress.track(
            reader.cells("normalized_url", with_rows=True), total=total
        ):
            parsed_url = parse_youtube_url(url)
            if isinstance(parsed_url, YoutubeVideo):
                video_writer.writerow(row)
            elif isinstance(parsed_url, YoutubeChannel):
                if parsed_url.id:
                    channel_writer.writerow(row + [parsed_url.id])
