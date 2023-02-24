import os

import duckdb

from CONSTANTS import (AGGREGATEDDOMAINSTABLE, LINKSTABLENAME, MAINTABLENAME)
from utils import Timer


def domains(connection):

    # Aggregate domains in a table
    timer = Timer('Aggregate URLs by domain')
    domain_columns = {
        'id':'VARCHAR',
        'domain': 'VARCHAR',
        'nb_links_from_domain':'UBIGINT',
        'nb_collected_tweets_with_domain':'UBIGINT',
        'nb_collected_retweets_with_domain':'UBIGINT',
        'sum_all_tweets_with_domain':'UBIGINT',
        'nb_distinct_accounts_shared_domain':'UBIGINT',
        'earliest_tweet':'DATETIME',
        'latest_tweet':'DATETIME',
        'days_between_first_and_last_tweet':'INTEGER',
        'histogram_of_tweets_per_month':'VARCHAR',
    }
    column_string = ', '.join([f'{k} {v}' for k,v in domain_columns.items()])
    connection.execute(f"""
    DROP TABLE IF EXISTS {AGGREGATEDDOMAINSTABLE}
    """)
    connection.execute(f"""
    CREATE TABLE {AGGREGATEDDOMAINSTABLE}({column_string})
    """)
    connection.execute(f"""
    INSERT INTO {AGGREGATEDDOMAINSTABLE}
    SELECT  c.domain_id,
            ANY_VALUE(c.domain_name),
            COUNT(DISTINCT c.distinct_links),
            COUNT(DISTINCT c.tweet_id)-COUNT(DISTINCT c.retweeted_id),
            COUNT(DISTINCT c.retweeted_id),
            COUNT(DISTINCT c.tweet_id),
            COUNT(DISTINCT c.user_id),
            min(local_time),
            max(local_time),
            datediff('day', min(local_time), max(local_time)),
            histogram(date_trunc('month', local_time)),
    FROM (
        SELECT  b.tweet_id,
                a.user_id,
                a.retweeted_id,
                a.quoted_id,
                b.domain_name,
                b.domain_id,
                b.distinct_links,
                a.local_time
        FROM {MAINTABLENAME} a
        JOIN (
            SELECT UNNEST(tweet_ids) as tweet_id, domain_id, domain_name, distinct_links
            FROM {LINKSTABLENAME}
        ) b
        ON b.tweet_id = a.id
    ) c
    WHERE c.domain_id IS NOT NULL
    GROUP BY c.domain_id
    """)

    duckdb.table(table_name=AGGREGATEDDOMAINSTABLE, connection=connection).order('nb_collected_tweets_with_domain')
    timer.stop()

    # Write the aggregated domains table to a CSV
    outfile_path = os.path.join('output', 'domains.csv')
    duckdb.table(
        table_name=AGGREGATEDDOMAINSTABLE,
        connection=connection
    ).to_csv(
        file_name=outfile_path,
        sep=',',
        header=True,
    )