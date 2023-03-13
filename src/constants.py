MAIN_TABLE_DATA_TYPES = {
    'tweet_id':'VARCHAR',
    'local_time':'DATETIME',
    'user_id':'VARCHAR',
    'retweeted_id':'VARCHAR',
    'link':'VARCHAR',
    'normalized_url':'VARCHAR',
    'domain_name':'VARCHAR'
    }

SELECT_COLUMNS = [
    'id',
    'local_time',
    'user_id',
    'retweeted_id',
    'links'
    ]

FREQUENCY_TABLE_DATA_TYPES = {
    'domain_id':'VARCHAR',
    'nb_tweets_per_month_with_domain':'UBIGINT'
}
FREQUENCY_TABLE = 'frequency_tbl'

DOMAIN_TABLE_DATA_TYPES = {
    'domain_id':'VARCHAR',
    'domain_name': 'VARCHAR',
    'nb_links_from_domain':'UBIGINT',
    'nb_collected_retweets_with_domain':'UBIGINT',
    'sum_all_tweets_with_domain':'UBIGINT',
    'nb_accounts_that_shared_domain_link':'UBIGINT',
    }

DOMAIN_TABLE = 'domains'
