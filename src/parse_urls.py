import duckdb
import pyarrow
from rich.progress import track
from ural import get_domain_name as ural_get_domain_name
from ural import normalize_url as ural_normalize_url
from ural.youtube import YOUTUBE_DOMAINS

from CONSTANTS import LINKSTABLENAME, MAINTABLENAME
from utils import Timer

association_table = 'link_tweet_relation'
parse_results_table = 'url_parse_results'


def parse_urls(connection):
    connection.execute('PRAGMA enable_progress_bar')

    # Extract individual URLs from a string concatenation
    timer = Timer('Relating links to tweets')
    connection.execute(f"""
    DROP TABLE IF EXISTS {association_table};
    """)
    connection.execute(f"""
    CREATE TABLE {association_table}(id BIGINT, link VARCHAR, tweet_id UBIGINT);
    """)
    connection.execute(f"""
    DROP SEQUENCE IF EXISTS seq0;
    """)
    connection.execute(f"""
    CREATE SEQUENCE seq0;
    """)
    connection.execute(f"""
    INSERT INTO {association_table}
    SELECT NEXTVAL('seq0'), UNNEST(link_list) as link, tweet_id
    FROM (
        SELECT STRING_SPLIT(s.links, '|') as link_list, id
        FROM (
            SELECT links, id
            FROM {MAINTABLENAME}
            WHERE links IS NOT NULL
        ) AS s
    ) tbl(link_list, tweet_id)
    WHERE LEN(link_list) > 0;
    """)
    link_tweet_pairs = duckdb.table(
        table_name=association_table,
        connection=connection
    )
    timer.stop()

    timer = Timer('In preparation of parsing, aggregating the links')
    link_aggregate = link_tweet_pairs.aggregate(
        'link, STRING_AGG(id)'
    ).fetchall()
    timer.stop()

    timer = Timer('Parsing extracted links with URAL')
    # Create empty column arrays
    link_list, normalized_url_list, domain_name_list, link_relation_ids_list = [], [], [], []
    for tuple in track(link_aggregate):
        # Parse the row data
        raw_url = str(tuple[0])
        norm_url = ural_normalize_url(raw_url)
        domain = ural_get_domain_name(norm_url)
        if domain in YOUTUBE_DOMAINS:
            domain = 'youtube.com'
        relation_ids = [int(i) for i in tuple[1].split(',')]
        for id in relation_ids:
            # Add row data to the column's array
            link_list.append(raw_url)
            normalized_url_list.append(norm_url)
            domain_name_list.append(domain)
            link_relation_ids_list.append(id)
    # Name columns
    table_column_names = [
        'normalized_url',
        'domain_name',
        'link',
        'id',
    ]
    # Create Arrow table
    timer.stop()

    timer = Timer('Writing parsed URL data to pyarrow table')
    aggregated_links_table = pyarrow.table(
        [
            pyarrow.array(normalized_url_list, type=pyarrow.string()),
            pyarrow.array(domain_name_list, type=pyarrow.string()),
            pyarrow.array(link_list, type=pyarrow.string()),
            pyarrow.array(link_relation_ids_list, type=pyarrow.int64()),
        ],
        names=table_column_names
    )
    timer.stop()

    timer = Timer('Creating SQL table from pyarrow table')
    duckdb.from_arrow(arrow_object=aggregated_links_table, connection=connection).create(table_name=parse_results_table)
    timer.stop()


def aggregating_links(connection):
    timer = Timer('Building enriched links table')
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
    DROP TABLE IF EXISTS temp;
    """)
    connection.execute(f"""
    CREATE TABLE temp(normalized_url VARCHAR, domain_name VARCHAR, link VARCHAR, tweet_id VARCHAR);
    """)
    connection.execute(f"""
    INSERT INTO temp
    SELECT  b.normalized_url,
            b.domain_name,
            b.link,
            a.tweet_id,
    FROM {association_table} a
    JOIN {parse_results_table} b
    ON a.id = b.id
    """)
    timer.stop()

    timer = Timer('Aggregating links in table')
    connection.execute(f"""
    DROP TABLE IF EXISTS {LINKSTABLENAME};
    """)
    connection.execute(f"""
    CREATE TABLE {LINKSTABLENAME}({links_columns_string});
    """)
    connection.execute(f"""
    INSERT INTO {LINKSTABLENAME}
    SELECT  md5(normalized_url) as id,
            normalized_url as normalized_url,
            ANY_VALUE(domain_name) as domain_name,
            md5(ANY_VALUE(domain_name)) as domain_id,
            ARRAY_AGG(tweet_id) as tweet_ids,
            ARRAY_AGG(DISTINCT link) as distinct_links,
    FROM temp
    GROUP BY normalized_url
    """)
    connection.execute(f"""
    DROP TABLE temp
    """)
    timer.stop()
