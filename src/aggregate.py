import os

import duckdb

from CONSTANTS import (AGGREGATEDDOMAINSTABLE, LINKSTABLENAME, MAINTABLENAME,
                       PREPROCESSDIR)
from utils import Timer


def domains(connection):

    # Establish every relation between a tweet and a domain
    timer = Timer('Relate tweets and domains')
    connection.execute(f"""
    CREATE SEQUENCE seq1;
    CREATE TABLE tweet_domain_relation(id BIGINT DEFAULT NEXTVAL('seq1'), tweet_id BIGINT, domain_id VARCHAR, domain_name VARCHAR);
    INSERT INTO tweet_domain_relation
    SELECT NEXTVAL('seq1'), UNNEST(tweet_ids), domain_id, domain_name
    FROM {LINKSTABLENAME};
    """)
    timer.stop()

    # Aggregate domains in a table
    timer = Timer('Aggregate URLs by domain')
    domain_columns = {
        'id':'VARCHAR',
        'domain': 'VARCHAR',
        'nb_total_tweets':'UBIGINT',
        'nb_original_tweets':'UBIGINT',
        'nb_retweets':'UBIGINT',
        'nb_users_distinct':'UBIGINT'
    }
    column_string = ', '.join([f'{k} {v}' for k,v in domain_columns.items()])
    connection.execute(f"""
    CREATE TABLE {AGGREGATEDDOMAINSTABLE}({column_string})
    """)
    connection.execute(f"""
    INSERT INTO {AGGREGATEDDOMAINSTABLE}
    SELECT  domain_id,
            ANY_VALUE(domain_name),
            LEN(STRING_AGG(tweet_id)),
            LEN(STRING_AGG(tweet_id))-LEN(STRING_AGG(retweeted_id))+0,
            LEN(STRING_AGG(retweeted_id)),
            LEN(STRING_AGG(DISTINCT user_id))
    FROM (
        SELECT  tweet_domain_relation.tweet_id,
                retweet_count,
                like_count,
                reply_count,
                user_id,
                user_followers,
                retweeted_id,
                quoted_id,
                links,
                domain_name,
                domain_id
        FROM tweet_domain_relation
        JOIN {MAINTABLENAME}
        ON tweet_domain_relation.tweet_id = {MAINTABLENAME}.id
    )
    GROUP BY domain_id
    """)
    duckdb.table(table_name=AGGREGATEDDOMAINSTABLE, connection=connection).order('nb_total_tweets')
    timer.stop()

    # Write the aggregated domains table to a CSV
    outfile_path = os.path.join(PREPROCESSDIR, 'domains.csv')
    duckdb.table(
        table_name=AGGREGATEDDOMAINSTABLE,
        connection=connection
    ).to_csv(
        file_name=outfile_path,
        sep=',',
        header=True,
    )