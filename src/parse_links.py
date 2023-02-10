import datetime
import timeit
import subprocess
from CONSTANTS import MAINTABLENAME



def run_subprocess(script, message:str):
    if isinstance(script,list):
        shell = False
    else:
        shell = True
    print("Running the {} command with arguments: {}".format(message, script))
    print('Began at {}'.format(datetime.datetime.now().time()))
    start = timeit.default_timer()
    subprocess.run(script, shell=shell, text=True)
    stop = timeit.default_timer()
    delta = stop - start
    print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))


def url_parser(con):

    # Create a table for links.
    links_table = 'links'
    con.execute(f"CREATE TABLE IF NOT EXISTS {links_table}(link VARCHAR, tweet_ids VARCHAR[]);")

    # First, explode the concatenated links into an array, with each array having one Tweet ID.
    # Then, 'unnest' the array's items onto separate rows, in which they keep their Tweet ID.
    # Finally, group the links together and aggregate each one's Tweet ID into an array.
    query = f"""
    INSERT INTO {links_table}
    SELECT link, [STRING_AGG(tweet_id)]
    FROM (
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
    ) GROUP BY link
    """
    print(f'\nCreating links table')
    print('Began at {}'.format(datetime.datetime.now().time()))
    start = timeit.default_timer()

    con.execute(query=query)

    stop = timeit.default_timer()
    delta = stop - start
    print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

    # Write the cleaned links table to CSV
    parsed_urls_csv = 'sql_links.csv'
    con.execute(f"COPY {links_table} TO '{parsed_urls_csv}' (HEADER, DELIMITER ',')")

    script=['minet', 'url-parse', '-o', 'parsed_urls.csv', 'link', parsed_urls_csv]
    message='Parsing unique URLs'
    run_subprocess(script=script, message=message)
