import json
import os
import duckdb

from ural import get_domain_name as ural_get_domain_name
from ural import normalize_url as ural_normalize_url

from CONSTANTS import LINKSTABLENAME, MAINTABLENAME, PREPROCESSDIR
from utils import Timer
from rich.progress import track


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
    links_list = duckdb.sql(query=query, connection=connection)
    link_aggregates = links_list.aggregate('link, STRING_AGG(tweet_id)')
    timer.stop()

    # Parse the extracted URLs
    timer = Timer('Parsing extracted links with URAL')
    normalized_links = {}
    for i in track(link_aggregates.fetchall()):
        raw_url = str(i[0])
        tweet_id = [int(id) for id in i[1].split(',')]
        norm_url = ural_normalize_url(raw_url)
        domain_name = ural_get_domain_name(norm_url)
        if not normalized_links.get(norm_url):
            normalized_links[norm_url] = {
                'normalized_url':norm_url,
                'domain_name':domain_name,
                'links':[raw_url],
                'tweet_ids':tweet_id
            }
        else:
            normalized_links[norm_url]['links'].append(raw_url)
            normalized_links[norm_url]['tweet_ids'].extend(tweet_id)
        normalized_links[norm_url]['links'] = list(set(normalized_links[norm_url]['links']))

    # Write the parsed URL data to a JSON
    normalized_links_json = os.path.join(PREPROCESSDIR, 'normalized_links.json')
    nested = [row for row in normalized_links.values()]
    with open(normalized_links_json, 'w') as fp:
        json.dump(nested, fp, indent=4)
    timer.stop()

    # Aggregate links by normalized URL
    timer = Timer('Import parsed URLs to links table')
    json_keys_as_columns = {
        'id':'VARCHAR PRIMARY KEY',
        'normalized_url':'VARCHAR',
        'domain_name':'VARCHAR',
        'domain_id':'VARCHAR',
        'tweet_ids':'BIGINT[]',
        'nb_tweets':'INTEGER',
        'links':'VARCHAR[]'
    }
    connection.execute(f"""
    CREATE TABLE {LINKSTABLENAME}({', '.join([f'{k} {v}' for k,v in json_keys_as_columns.items()])})
    """)
    connection.execute(f"""
    INSERT INTO {LINKSTABLENAME}
    SELECT  md5(record.normalized_url),
            record.normalized_url,
            record.domain_name,
            md5(record.domain_name),
            record.tweet_ids,
            len(record.tweet_ids),
            record.links
    FROM (
        SELECT UNNEST(json) AS record
        FROM (
            SELECT *
            FROM read_json_auto('{normalized_links_json}', maximum_depth=-1)
        ) json
    )
    """)
    # Clean up the JSON serlialization of the parsed URL data
    os.remove(normalized_links_json)
    timer.stop()
