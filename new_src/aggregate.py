import duckdb

from CONSTANTS import MAINTABLENAME, DOMAINTABLE, FREQUENCYTABLE, JOINEDDOMAINFREQUENCYTABLE
from utils import Timer

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

def domains(connection):

    # Aggregate domains in a table
    timer = Timer('Aggregating URLs by domain')
    column_string = ', '.join([f'{k} {v}' for k,v in domain_columns.items()])
    connection.execute(f"""
    DROP TABLE IF EXISTS {DOMAINTABLE}
    """)
    connection.execute(f"""
    CREATE TABLE {DOMAINTABLE}({column_string})
    """)
    connection.execute(f"""
    INSERT INTO {DOMAINTABLE}
    SELECT  md5(domain_name),
            domain_name,
            COUNT(DISTINCT link),
            COUNT(DISTINCT tweet_id)-COUNT(DISTINCT retweeted_id),
            COUNT(DISTINCT retweeted_id),
            COUNT(DISTINCT tweet_id),
            COUNT(DISTINCT user_id),
            min(local_time),
            max(local_time),
            datediff('day', min(local_time), max(local_time)),
            histogram(date_trunc('month', local_time)),
    FROM {MAINTABLENAME}
    WHERE domain_name IS NOT NULL
    GROUP BY domain_name
    """)
    # connection.execute(f"""
    # CREATE INDEX domain_idx ON {DOMAINTABLE} (id);
    # """)
    timer.stop()

def tweets_per_month(connection):
    timer = Timer('Calculating average number of tweets per month per domain')
    connection.execute(f"""
    DROP TABLE IF EXISTS {FREQUENCYTABLE};
    """)
    connection.execute(f"""
    CREATE TABLE {FREQUENCYTABLE}(domain_id VARCHAR, nb_tweets_per_month_with_domain UBIGINT);
    """)
    connection.execute(f"""
    INSERT INTO {FREQUENCYTABLE}
    SELECT md5(domain_name), COUNT(tweet_id)
    FROM {MAINTABLENAME}
    GROUP BY DATEPART('month', local_time), domain_name
    """)
    # connection.execute(f"""
    # CREATE INDEX frequency_idx ON {FREQUENCYTABLE} (domain_id);
    # """)
    timer.stop()

    timer = Timer('Joining frequencies to domain aggregation')
    connection.execute(f"""
    DROP TABLE IF EXISTS {JOINEDDOMAINFREQUENCYTABLE};
    """)
    column_string = ', '.join([f'{k} {v}' for k,v in domain_columns.items()])
    connection.execute(f"""
    CREATE TABLE IF NOT EXISTS {JOINEDDOMAINFREQUENCYTABLE}({column_string}, nb_tweets_per_month_with_domain UBIGINT)
    """)
    selected_domain_columns = ', '.join([f'a.{k}' for k in domain_columns.keys()])
    connection.execute(f"""
    INSERT INTO {JOINEDDOMAINFREQUENCYTABLE}
    SELECT {selected_domain_columns}, b.nb_tweets_per_month_with_domain
    FROM {DOMAINTABLE} a
    INNER JOIN {FREQUENCYTABLE} b
    ON a.id = b.domain_id
    """)
    timer.stop()
