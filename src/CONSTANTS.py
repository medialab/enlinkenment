import os

MAINTABLENAME = 'tweets'
LINKSTABLENAME = 'links'
PARSEDLINKSTABLE = 'parsed_links'
AGGREGATEDPARSEDLINKSTABLE = 'aggregated_parsed_links'
AGGREGATEDDOMAINSTABLE = 'aggregated_domains'

DEFAULTDATABASE = os.path.join('database', 'tweet_links.db')

TWEET = {
    'id':'UBIGINT PRIMARY KEY',
    'timestamp_utc':'UBIGINT',
    'local_time':'TIMESTAMP',
    'user_screen_name':'VARCHAR',
    'text':'VARCHAR',
    'possibly_sensitive':'BOOLEAN',
    'retweet_count':'INTEGER',
    'like_count':'INTEGER',
    'reply_count':'INTEGER',
    'lang':'VARCHAR',
    'to_username':'VARCHAR',
    'to_userid':'UBIGINT',
    'to_tweetid':'UBIGINT',
    'source_name':'VARCHAR',
    'source_url':'VARCHAR',
    'user_location':'VARCHAR',
    'lat':'VARCHAR',
    'lng':'VARCHAR',
    'user_id':'UBIGINT',
    'user_name':'VARCHAR',
    'user_verified':'BOOLEAN',
    'user_description':'VARCHAR',
    'user_url':'VARCHAR',
    'user_image':'VARCHAR',
    'user_tweets':'INTEGER',
    'user_followers':'INTEGER',
    'user_friends':'INTEGER',
    'user_likes':'INTEGER',
    'user_lists':'INTEGER',
    'user_created_at':'TIMESTAMP',
    'user_timestamp_utc':'UBIGINT',
    'collected_via':'VARCHAR',
    'match_query':'INTEGER',
    'retweeted_id':'UBIGINT',
    'retweeted_user':'VARCHAR',
    'retweeted_user_id':'UBIGINT',
    'retweeted_timestamp_utc':'UBIGINT',
    'quoted_id':'UBIGINT',
    'quoted_user':'VARCHAR',
    'quoted_user_id':'UBIGINT',
    'quoted_timestamp_utc':'UBIGINT',
    'collection_time':'TIMESTAMP',
    'url':'VARCHAR',
    'place_country_code':'VARCHAR',
    'place_name':'VARCHAR',
    'place_type':'VARCHAR',
    'place_coordinates':'VARCHAR',
    'links':'VARCHAR',
    'domains':'VARCHAR',
    'media_urls':'VARCHAR',
    'media_files':'VARCHAR',
    'media_types':'VARCHAR',
    'mentioned_names':'VARCHAR',
    'mentioned_ids':'VARCHAR',
    'hashtags':'VARCHAR'
}


TWEET_REDUCED = {
    'id':'VARCHAR PRIMARY KEY',
    'timestamp_utc':'UBIGINT',
    'retweet_count':'INTEGER',
    'like_count':'INTEGER',
    'reply_count':'INTEGER',
    'user_id':'VARCHAR',
    'user_followers':'INTEGER',
    'user_friends':'INTEGER',
    'retweeted_id':'VARCHAR',
    'retweeted_user_id':'VARCHAR',
    'quoted_id':'VARCHAR',
    'quoted_user_id':'VARCHAR',
    'links':'VARCHAR',
}

REDUCED_TWEET_COLUMNS = ','.join([k for k in TWEET_REDUCED.keys()])
XSV_CSV = os.path.join('temp','select_tweet_columns.csv')
XSV_SCRIPT = ['xsv', 'select', '-o', XSV_CSV, REDUCED_TWEET_COLUMNS]

URL_PARSE_INFILE = os.path.join('temp', 'url-parse_infile.csv')
URL_PARSE_OUTFILE = os.path.join('temp','url-parse_outfile.csv')

TEMPFILE = os.path.join('temp', 'temp.csv')
DOMAIN_PARSE_INFILE = os.path.join('temp', 'domain-parse_infile.csv')
DOMAIN_PARSE_OUTFILE = os.path.join('temp', 'domain-parse_outfile.csv')