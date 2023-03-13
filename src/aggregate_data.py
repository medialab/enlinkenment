import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from constants import DOMAIN_TABLE, DOMAIN_TABLE_DATA_TYPES
from utils import list_tables, pair_tables

DOMAIN_AGGREGATION = """
md5(domain_name) AS domain_id,
domain_name AS domain_name,
COUNT(DISTINCT link) AS nb_links_from_domain,
COUNT(DISTINCT retweeted_id) AS nb_collected_retweets_with_domain,
COUNT(DISTINCT tweet_id) AS sum_all_tweets_with_domain,
COUNT(DISTINCT user_id) AS nb_accounts_that_shared_domain_link,
"""


def aggregate_domains(connection):
    """Function to aggregate domains in main table."""

    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        tables = list_tables(connection)
        task_id = progress.add_task('Aggregating months...', total=len(tables))

        columns = ', '.join([f'{k} {v}' for k,v in DOMAIN_TABLE_DATA_TYPES.items()])
        for table in tables:
            agg_table = f'domains_in_{table}'
            connection.execute(f"""
            CREATE TABLE {agg_table}({columns});
            """)
            connection.execute(f"""
            INSERT INTO {agg_table}
            SELECT {DOMAIN_AGGREGATION}
            FROM {table}
            GROUP BY domain_name;
            """)
            print(f'created_table: {agg_table}')
            print(duckdb.table(table_name=agg_table, connection=connection).describe())
            connection.execute(f"""
            DROP TABLE {table};
            """)
            progress.update(task_id=task_id, advance=1)


def sum_aggregates(connection):

    tables = list_tables(connection)

    while len(tables) > 1:
        pairs = pair_tables(tables)
        print('\nAggregate groupings:')
        for i, pair in enumerate(pairs):
            i+=1
            if isinstance(pair, list):
                print(f'    n{i}: {pair[0]} + {pair[1]}')
            else:
                print(f'    n{i}: {pair}')

        columns = ', '.join([f'{k} {v}' for k,v in DOMAIN_TABLE_DATA_TYPES.items()])

        ProgressCompleteColumn = Progress(
        TextColumn("{task.description}"),
        MofNCompleteColumn(),
        BarColumn(bar_width=60),
        TimeElapsedColumn(),
        expand=True,
        )
        with ProgressCompleteColumn as progress:
            task_id = progress.add_task('Summing aggregated fields...', total=len(pairs))
            for pair in pairs:
                if len(pair) == 2:
                    left_table = pair[0]
                    right_table = pair[1]
                    new_table_name = '{}_{}'.format(left_table.split('_')[-1], right_table.split('_')[-1])
                    connection.execute(f"""
                    CREATE TABLE {new_table_name}({columns});
                    """)
                    connection.execute(f"""
                    INSERT INTO {left_table}
                    SELECT *
                    FROM {right_table};
                    """)
                    connection.execute(f"""
                    INSERT INTO {new_table_name}
                    SELECT  domain_id,
                            ANY_VALUE(domain_name),
                            SUM(nb_links_from_domain) AS nb_links_from_domain,
                            SUM(nb_collected_retweets_with_domain) AS nb_collected_retweets_with_domain,
                            SUM(sum_all_tweets_with_domain) AS sum_all_tweets_with_domain,
                            SUM(nb_accounts_that_shared_domain_link) AS nb_accounts_that_shared_domain_link
                    FROM {left_table}
                    GROUP BY domain_id
                    """)
                    connection.execute(f"""
                    DROP TABLE {left_table};
                    DROP TABLE {right_table};
                    """)
                progress.update(task_id=task_id, advance=1)
                tables = list_tables(connection)

    print("\nCopying summed aggregates to domains table")
    connection.execute('PRAGMA disable_progress_bar')
    connection.execute(f"""
    CREATE TABLE {DOMAIN_TABLE}({columns});
    """)
    connection.execute(f"""
    INSERT INTO {DOMAIN_TABLE}
    SELECT *
    FROM {tables[0]};
    """)

