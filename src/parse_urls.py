import os

import duckdb
import pyarrow
import pyarrow.parquet as pq
from rich.progress import track
from ural import get_domain_name as ural_get_domain_name
from ural import normalize_url as ural_normalize_url

from CONSTANTS import LINKSTABLENAME, MAINTABLENAME, PREPROCESSDIR
from utils import Timer


def parse_urls(connection):
    connection.execute('PRAGMA enable_progress_bar')

    # Extract individual URLs from a string concatenation
    timer = Timer('Extracting links and tweet IDs')
    query = f"""
    SELECT UNNEST(link_list) as link, tweet_id
    FROM (
        SELECT STRING_SPLIT(s.links, '|') as link_list, id
        FROM (
            SELECT links, id
            FROM {MAINTABLENAME}
            WHERE links IS NOT NULL
        ) AS s
    ) tbl(link_list, tweet_id)
    """
    links_list = duckdb.sql(query=query, connection=connection).fetchall()
    timer.stop()

    timer = Timer('Parsing extracted links with URAL')
    # Create empty column arrays
    link_list, tweet_ids_list, normalized_url_list, domain_name_list = [], [], [], []
    for tuple in track(links_list):
        # Parse the row data
        raw_url = str(tuple[0])
        norm_url = ural_normalize_url(raw_url)
        domain = ural_get_domain_name(norm_url)
        tweet_id = int(tuple[1])
        # Add row data to the column's array
        link_list.append(raw_url)
        tweet_ids_list.append(tweet_id)
        normalized_url_list.append(norm_url)
        domain_name_list.append(domain)
    # Name columns
    table_column_names = [
        'normalized_url',
        'domain_name',
        'link',
        'tweet_id',
    ]
    # Create Arrow table
    timer.stop()
    timer = Timer('Writing parsed URL data to parquet file')
    aggregated_links_table = pyarrow.table(
        [
            pyarrow.array(normalized_url_list, type=pyarrow.string()),
            pyarrow.array(domain_name_list, type=pyarrow.string()),
            pyarrow.array(link_list, type=pyarrow.string()),
            pyarrow.array(tweet_ids_list, type=pyarrow.int64()),
        ],
        names=table_column_names
    )

    # Write table to a parquet file
    parquet_file = os.path.join(PREPROCESSDIR, 'aggregate.parquet')
    pq.write_table(
        aggregated_links_table,
        parquet_file,
        compression='gzip'
    )
    timer.stop()

    timer = Timer('Importing parquet file to database')
    # Read parquet file to DuckDB table
    links_column_dict = {
        'id':'VARCHAR PRIMARY KEY',
        'normalized_url':'VARCHAR',
        'domain_name':'VARCHAR',
        'domain_id':'VARCHAR',
        'tweet_ids':'BIGINT[]',
        'distinct_links':'VARCHAR[]',
    }
    links_columns_string = ', '.join(f'{k} {v}' for k,v in links_column_dict.items())
    connection.execute(f"""
    CREATE TABLE {LINKSTABLENAME}({links_columns_string});
    """)
    connection.execute(f"""
    INSERT INTO {LINKSTABLENAME}
    SELECT  md5(normalized_url),
            normalized_url,
            ANY_VALUE(domain_name),
            md5(ANY_VALUE(domain_name)),
            ARRAY_AGG(tweet_id),
            ARRAY_AGG(DISTINCT link),
    FROM read_parquet('{parquet_file}')
    GROUP BY normalized_url
    """)
    timer.stop()
    timer = Timer('Counting sums in aggregated links table')
    print('Counting number of tweets per link')
    connection.execute(f"""
    ALTER TABLE {LINKSTABLENAME} ADD COLUMN nb_tweets INTEGER DEFAULT 0;
    UPDATE {LINKSTABLENAME} SET nb_tweets = LEN(tweet_ids);
    """)
    print('Counting number of link variations')
    connection.execute(f"""
    ALTER TABLE {LINKSTABLENAME} ADD COLUMN nb_distinct_links INTEGER DEFAULT 0;
    UPDATE {LINKSTABLENAME} SET nb_distinct_links = LEN(distinct_links);
    """)
    timer.stop()
