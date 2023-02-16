import gzip
import json
import os
import subprocess
from pathlib import Path

import click
import duckdb
from ural import get_domain_name, normalize_url

from CONSTANTS import (AGGREGATEDDOMAINSTABLE, DEFAULTDATABASE, LINKSTABLENAME,
                       MAINTABLENAME, TWEET_REDUCED, XSV_CSV, XSV_SCRIPT)
from utils import Timer


@click.command()
@click.option('-i', '--input', required=True)
@click.option('-d', '--database-dir', required=False)
def main(input, database_dir):

    # Set up a database in which to store everything
    if not database_dir or Path(database_dir).is_file():
        os.makedirs('database', exist_ok=True)
        database = DEFAULTDATABASE
    else:
        os.makedirs(database_dir)
        database = os.path.join(database_dir, 'tweet_links.db')

    # Connect to the database
    connection = duckdb.connect(database=database, read_only=False)

    # Create the main table for the input tweet data.
    timer = Timer('Creating main table')
    timer.start()
    fields = TWEET_REDUCED
    columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
    connection.execute(f"""
    CREATE TABLE IF NOT EXISTS {MAINTABLENAME}({columns})
    """)
    timer.stop()

    # Create a directory in which to store temporary files
    os.makedirs('temp', exist_ok=True)

    # Get data files' paths.
    input_path = Path(input)
    if input_path.is_file():
        file_path_objects = [input_path]
    elif input_path.is_dir():
        file_path_objects = [x for x in input_path.iterdir() if x.is_file() if x.suffix == '.csv' or x.suffix == '.gz']
    else:
        raise FileExistsError()

    # Iterate through the input
    for path_obj in file_path_objects:
        # Preprocess the in-file to extract only the necessary columns
        with gzip.open(str(path_obj), 'r') as f:
            try:
                f.read(1)
            except:
                shell = False
                script = XSV_SCRIPT+[str(path_obj)]
            else:
                shell = True
                script = " ".join(['gzcat', str(path_obj), '|']+XSV_SCRIPT)
        timer = Timer(f'Selecting columns from {path_obj.name} with xsv')
        timer.start()
        subprocess.run(script, shell=shell)
        timer.stop()

        # Import the selected columns' data into the database
        timer = Timer(f'Importing data from {path_obj.name}')
        timer.start()
        fields = TWEET_REDUCED
        fields.update({'id':'UBIGINT'})
        columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
        connection.execute(f"""
        DROP TABLE IF EXISTS input
        """)
        connection.execute(f"""
        CREATE TABLE IF NOT EXISTS input({columns})
        """)
        connection.execute(f"""
        INSERT INTO input
        SELECT *
        FROM read_csv('{XSV_CSV}', delim=',', header=True, columns={fields});
        """)
        connection.execute(f"""
        COPY input TO 'temp/{path_obj.name}' (HEADER, DELIMITER ',')
        """)
        # Clean up xsv select columns
        os.remove(XSV_CSV)
        timer.stop()

        # Update the main tweets table with the new data
        timer = Timer(f'Merging imported data from {path_obj.name} into "{MAINTABLENAME}".')
        timer.start()
        connection.execute(f"""
        INSERT INTO {MAINTABLENAME}
        SELECT *
        FROM input
        WHERE input.id NOT IN (SELECT id FROM {MAINTABLENAME});
        """)
        connection.execute(f"DROP TABLE input")
        timer.stop()

    # Extract all the tweets' links from string concatenations
    timer = Timer('Extracting links and tweet IDs')
    timer.start()
    connection.execute(f"""
    SELECT UNNEST(link_list) as link, tweet_id
    FROM (
        SELECT link_list, id as tweet_id
        FROM (
            SELECT STRING_SPLIT(s.links, '|') as link_list, id
            FROM (
                SELECT links, id
                FROM {MAINTABLENAME}
                WHERE links IS NOT NULL
            ) AS s
        )
    ) tbl(link_list, tweet_id)
    """)
    timer.stop()

    # Parse URLs extracted from tweets table
    timer = Timer('Parsing extracted links')
    timer.start()
    links_table = connection.fetchnumpy()
    links_array = list(zip(links_table['link'], links_table['tweet_id']))
    normalized_links = {}
    for i in links_array:
        raw_url = i[0]
        tweet_id = i[1]
        norm_url = normalize_url(raw_url)
        domain_name = get_domain_name(norm_url)
        if not normalized_links.get(norm_url):
            normalized_links[norm_url] = {
                'normalized_url':norm_url,
                'domain_name':domain_name,
                'links':[raw_url],
                'tweet_ids':[tweet_id]
            }
        else:
            normalized_links[norm_url]['links'].append(raw_url)
            normalized_links[norm_url]['tweet_ids'].append(tweet_id)
        normalized_links[norm_url]['links'] = list(set(normalized_links[norm_url]['links']))
    normalized_links_json = os.path.join('temp', 'normalized_links.json')

    # Write the parsed URL data to a JSON
    nested = [row for row in normalized_links.values()]
    with open(normalized_links_json, 'w') as fp:
        json.dump(nested, fp)
    timer.stop()

    # Aggregate links by normalized URL
    timer = Timer('Import parsed URLs to links table')
    timer.start()
    links_json_columns = {'id':'VARCHAR PRIMARY KEY', 'normalized_url':'VARCHAR', 'domain_name':'VARCHAR', 'domain_id':'VARCHAR', 'tweet_ids':'BIGINT[]', 'nb_tweets':'INTEGER', 'links':'VARCHAR[]'}
    connection.execute(f"""
    CREATE TABLE {LINKSTABLENAME}({', '.join([f'{k} {v}' for k,v in links_json_columns.items()])})
    """)
    connection.execute(f"""
    INSERT INTO {LINKSTABLENAME}
    SELECT md5(record.normalized_url), record.normalized_url, record.domain_name, md5(record.domain_name), record.tweet_ids, len(record.tweet_ids), record.links
    FROM (
        SELECT UNNEST(json) AS record
        FROM (
            SELECT *
            FROM read_json_auto('{normalized_links_json}', maximum_depth=-1)
        ) json
    )
    """)
    timer.stop()

    # Establish every relation between a tweet and a domain 
    timer = Timer('Relate tweets and domains')
    timer.start()
    connection.execute(f"""
    CREATE SEQUENCE seq1;
    CREATE TABLE tweet_domain_relation(id BIGINT DEFAULT NEXTVAL('seq1'), tweet_id BIGINT, domain_id VARCHAR, domain_name VARCHAR);
    INSERT INTO tweet_domain_relation
    SELECT NEXTVAL('seq1'), UNNEST(tweet_ids), domain_id, domain_name
    FROM {LINKSTABLENAME};
    """)
    timer.stop()

    # Establish every relation between a user and a domain
    timer = Timer('Relate users and domains')
    timer.start()
    connection.execute(f"""
    CREATE SEQUENCE seq2;
    CREATE TABLE user_domain_relation(id BIGINT DEFAULT NEXTVAL('seq2'), user_id BIGINT, domain_id VARCHAR, domain_name VARCHAR);
    INSERT INTO user_domain_relation
    SELECT NEXTVAL('seq2'), user_id, domain_id, domain_name
    FROM (
        SELECT id as tweet_id, user_id
        FROM {MAINTABLENAME}
    ) s
    JOIN tweet_domain_relation
    ON s.tweet_id = tweet_domain_relation.tweet_id;
    """)

    # Aggregate domains in a table
    timer = Timer('Aggregate URLs by domain')
    timer.start()
    domains_columns = {'id':'VARCHAR', 'domain': 'VARCHAR', 'nb_total_tweets':'INTEGER', 'nb_original_tweets':'INTEGER', 'nb_retweets':'INTEGER', 'nb_users':'INTEGER'}
    column_string = ', '.join([f'{k} {v}' for k,v in domains_columns.items()])
    connection.execute(f"""
    CREATE TABLE {AGGREGATEDDOMAINSTABLE}({column_string})
    """)
    # id,timestamp_utc,retweet_count,like_count,reply_count,user_id,user_followers,user_friends,retweeted_id,retweeted_user_id,quoted_id,quoted_user_id,links
    connection.execute(f"""
    INSERT INTO {AGGREGATEDDOMAINSTABLE}
    SELECT domain_id, ANY_VALUE(domain_name), LEN(STRING_AGG(tweet_id)), LEN(STRING_AGG(tweet_id))-LEN(STRING_AGG(retweeted_id)), LEN(STRING_AGG(retweeted_id)), LEN(STRING_AGG(user_id))
    FROM (
        SELECT tweet_domain_relation.tweet_id, retweet_count, like_count, reply_count, user_id, user_followers, retweeted_id, quoted_id, links, domain_name, domain_id
        FROM tweet_domain_relation
        JOIN {MAINTABLENAME}
        ON tweet_domain_relation.tweet_id = {MAINTABLENAME}.id
    )
    GROUP BY domain_id
    """)
    timer.stop()

    from pprint import pprint
    pprint(connection.fetchall())

    connection.execute(f"COPY {AGGREGATEDDOMAINSTABLE} TO 'output.csv' (HEADER, DELIMITER ',');")



if __name__ == "__main__":
    main()
