from CONSTANTS import DOMAINTABLE, FREQUENCYTABLE, MAINTABLENAME
from utils import Timer
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
import duckdb
import string
import itertools
from import_data import main_columns

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

letters_numbers = list(string.ascii_letters)+['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '-', '.']
combinations = itertools.combinations(letters_numbers, 2)
patterns = [f'{tuple[0]}{tuple[1]}' for tuple in combinations]

def domains(connection):

    # Set up concatenated domain aggregates table
    column_string = ', '.join([f'{k} {v}' for k,v in domain_columns.items()])
    connection.execute(f"""
    DROP TABLE IF EXISTS {DOMAINTABLE}
    """)
    connection.execute(f"""
    CREATE TABLE {DOMAINTABLE}({column_string})
    """)

    # Try to aggregate all domains at once
    try:
        connection.execute('PRAGMA enable_progress_bar')
        timer = Timer('Aggregating domains')
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
        timer.stop()

        timer = Timer('Aggregating by domain and publication time')
        connection.execute(f"""
        DROP TABLE IF EXISTS {FREQUENCYTABLE};
        """)
        connection.execute(f"""
        CREATE TABLE {FREQUENCYTABLE}(domain_name VARCHAR, nb_tweets_per_month_with_domain UBIGINT);
        """)
        connection.execute(f"""
        INSERT INTO {FREQUENCYTABLE}
        SELECT domain_name, COUNT(tweet_id)
        FROM {MAINTABLENAME}
        GROUP BY DATEPART('month', local_time), domain_name
        """)
        timer.stop()

    except:
        print('Too large for memory. Switching to chunked process.')

        # Undo first try
        print('Resetting domain aggregates table.')
        connection.execute(f"""
        DROP TABLE IF EXISTS {DOMAINTABLE}
        """)
        connection.execute(f"""
        CREATE TABLE {DOMAINTABLE}({column_string})
        """)
        connection.execute(f"""
        DROP TABLE IF EXISTS {FREQUENCYTABLE};
        """)
        connection.execute(f"""
        CREATE TABLE {FREQUENCYTABLE}(domain_name VARCHAR, nb_tweets_per_month_with_domain UBIGINT);
        """)

        # Create a relation without twitter.com
        print(f'\nOrdering table by domain name to improve chunking.')
        duckdb.table(MAINTABLENAME, connection=connection).order('domain_name')
        main_columns_string = ', '.join([f'{k} {v}' for k,v in main_columns.items()])
        connection.execute(f"""
        DROP TABLE IF EXISTS tweets_without_twitter;
        """)
        connection.execute(f"""
        CREATE TABLE tweets_without_twitter({main_columns_string});
        """)
        timer = Timer('Creating relation without twitter.com domain')
        connection.execute(f"""
        INSERT INTO tweets_without_twitter
        SELECT *
        FROM {MAINTABLENAME}
        WHERE domain_name != 'twitter.com';
        """)
        timer.stop()

        connection.execute('PRAGMA disable_progress_bar')

        timer = Timer('Aggregating chunks of domains')
        text_column = TextColumn("{task.description}")
        bar_column = BarColumn(bar_width=60)
        time_remaining_column = TimeRemainingColumn()
        completed_column = MofNCompleteColumn()

        progress = Progress(
            text_column,
            completed_column,
            bar_column,
            time_remaining_column,
            expand=True,
            )
        with progress:
            task1 = progress.add_task('[red]Chunking...', total=len(patterns))
            task2 = progress.add_task('[green]Aggregating by domain...', total=len(patterns))
            task3 = progress.add_task('[blue]Aggregating by domain and publication time...', total=len(patterns))

            while not progress.finished:
                for pattern in patterns:
                    # Chunking data according to pattern
                    chunk = duckdb.sql(f"""
                    SELECT *
                    FROM tweets_without_twitter
                    WHERE regexp_matches(domain_name, '^{pattern}')
                    """,
                    connection=connection)
                    progress.update(task1, advance=1)

                    # Perform aggregate functions on domain group table 
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
                    FROM chunk
                    GROUP BY domain_name
                    """)
                    progress.update(task2, advance=1)
                    connection.execute(f"""
                    INSERT INTO {FREQUENCYTABLE}
                    SELECT domain_name, COUNT(tweet_id)
                    FROM chunk
                    GROUP BY DATEPART('month', local_time), domain_name
                    """)
                    progress.update(task3, advance=1)

        timer.stop()
