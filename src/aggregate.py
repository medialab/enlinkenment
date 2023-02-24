import ast
import os
import re

import duckdb
import matplotlib.pyplot as plt

from CONSTANTS import AGGREGATEDDOMAINSTABLE, LINKSTABLENAME, MAINTABLENAME
from utils import Timer


def domains(connection):

    # Aggregate domains in a table
    timer = Timer('Aggregating URLs by domain')
    domain_columns = {
        'id':'VARCHAR',
        'domain': 'VARCHAR',
        'nb_links_from_domain':'UBIGINT',
        'nb_collected_tweets_with_domain':'UBIGINT',
        'nb_collected_retweets_with_domain':'UBIGINT',
        'sum_all_tweets_with_domain':'UBIGINT',
        'nb_accounts_that_shared_domain_link':'UBIGINT',
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
    timer.stop()

    timer = Timer('Calculating average number of tweets per month per domain')
    connection.execute(f"""
    ALTER TABLE {AGGREGATEDDOMAINSTABLE}
        ADD COLUMN nb_tweets_per_month_with_domain UBIGINT;
    """)
    connection.execute(f"""
    UPDATE {AGGREGATEDDOMAINSTABLE}
        SET nb_tweets_per_month_with_domain = s.count_per_month
        FROM (
            SELECT  COUNT(tweet_id) as count_per_month, domain_id
            FROM (
                SELECT  b.tweet_id,
                        a.local_time,
                        b.domain_id,
                        b.domain_name
                FROM {MAINTABLENAME} a
                JOIN (
                    SELECT UNNEST(tweet_ids) as tweet_id, domain_id, domain_name
                    FROM {LINKSTABLENAME}
                ) b
                ON b.tweet_id = a.id
            ) c
            WHERE c.domain_id IS NOT NULL
            GROUP BY DATEPART('month', local_time), domain_id
        ) s
        WHERE {AGGREGATEDDOMAINSTABLE}.id = s.domain_id;
    """)
    duckdb.table(table_name=AGGREGATEDDOMAINSTABLE, connection=connection).order('nb_collected_tweets_with_domain')
    timer.stop()

    outfile_path = os.path.join('output', 'domains.csv')
    timer = Timer(f'Writing CSV to {outfile_path}')
    duckdb.table(
        table_name=AGGREGATEDDOMAINSTABLE,
        connection=connection
    ).to_csv(
        file_name=outfile_path,
        sep=',',
        header=True,
    )
    timer.stop()

    timer = Timer('Writing histograms for high-volume domains')
    data = duckdb.query(f"""
    SELECT histogram_of_tweets_per_month, domain 
    FROM {AGGREGATEDDOMAINSTABLE}
    """, connection=connection).fetchall()
    max_per_month = duckdb.table(
        table_name=AGGREGATEDDOMAINSTABLE,
        connection=connection).max('nb_tweets_per_month_with_domain').fetchone()[0]
    histogram_dir = os.path.join('output', 'histograms')
    os.makedirs(histogram_dir, exist_ok=True)
    for tuple in data:
        domain = tuple[1]
        histogram = str(tuple[0]).replace('=',':')
        histogram = re.sub(
                        pattern=r'(\d{4}-\d{2}-\d{2})', 
                        repl='"\\1"', 
                        string=histogram
                    )
        dictionary = ast.literal_eval(histogram)
        if len(list(dictionary.keys())) > 6 and sum(list(dictionary.values())) > 2000:
            plt.title(f"Tweets per month for {domain}")
            # x = [datetime.strptime(k, '%Y-%m-%d') for k in list(dictionary.keys())]
            x = list(dictionary.keys())
            y = dictionary.values()
            plt.bar(x, y, color='g')
            plt.ylim(-2, max_per_month+10)
            outfile_path = os.path.join(histogram_dir, f'{domain}.png')
            plt.savefig(outfile_path)
    timer.stop()
