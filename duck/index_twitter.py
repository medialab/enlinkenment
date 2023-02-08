from pathlib import Path
import datetime
import timeit
import subprocess

import click
import duckdb

from data_fields import TWEET


def run_subprocess(shell:bool, script, message:str):
    if isinstance(script,list):
        script_name = ' '.join(script)
    else:
        script_name = script
    print("Running the {} command: {}".format(message, script_name))
    print('Began at {}'.format(datetime.datetime.now().time()))
    start = timeit.default_timer()
    subprocess.run(script, shell=shell, text=True)
    stop = timeit.default_timer()
    delta = stop - start
    print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))


@click.command()
@click.argument('data')
def main(data):

    # Get data files paths and names.
    data_path = Path(data)
    if data_path.is_file():
        filepaths = [data_path]
    elif data_path.is_dir():
        filepaths = [x for x in data_path.iterdir() if x.is_file() and x.suffix == '.csv' or x.suffix == '.gz']
    else:
        raise FileExistsError()

    # Connect to the SQL database.
    con = duckdb.connect(database='tweets.db', read_only=False)

    # Create the main table of tweets.
    main_table = 'tweets'
    fields = TWEET
    columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
    con.execute(f"CREATE TABLE IF NOT EXISTS {main_table}({columns})")

    # Create a table for links.
    links_table = 'links'
    con.execute(f"CREATE TABLE IF NOT EXISTS {links_table}(link VARCHAR, tweet_ids VARCHAR[]);")

    # Iterate through the data files and add them to the main table.
    for file in filepaths:
        print(f'\nCreating table of {file.name}')
        print('Began at {}'.format(datetime.datetime.now().time()))
        start = timeit.default_timer()

        table = 'input'
        fields = TWEET
        columns = ', '.join([f'{k} {v}' for k,v in fields.items()])
        con.execute(f"CREATE TABLE {table}({columns})")
        con.execute(f"COPY {table} FROM '{file}' (HEADER);")
        con.execute(f"INSERT INTO {main_table} SELECT {table}.* FROM {table} FULL JOIN {main_table} ON {table}.id = {main_table}.id WHERE {main_table}.id IS NULL;")
        con.execute(f"DROP TABLE {table}")

        stop = timeit.default_timer()
        delta = stop - start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

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
                    FROM {main_table}
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


    shell=False
    script=['minet', 'url-parse', '-o', 'parsed_urls.csv', 'link', parsed_urls_csv]
    message='Parsing unique URLs'
    run_subprocess(shell=shell, script=script, message=message)

if __name__ == "__main__":
    main()
