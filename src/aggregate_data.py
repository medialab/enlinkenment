import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from constants import DOMAIN_TABLE, DOMAIN_TABLE_DATA_TYPES
from utils import list_tables, pair_tables

DOMAIN_AGGREGATION = """
md5(domain_name),
domain_name,
COUNT(DISTINCT normalized_url),
COUNT(DISTINCT retweeted_id),
COUNT(DISTINCT tweet_id),
COUNT(DISTINCT user_id),
"""

def aggregate_domains(connection, months):
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

        base_domain_columns = ', '.join([f'{k} {v}' for k,v in DOMAIN_TABLE_DATA_TYPES.items()])
        month_columns = ', '.join([f'nb_tweets_in_{month} UBIGINT' for _, month in months])
        columns = base_domain_columns+', '+month_columns

        for table in tables:
            agg_table = f'domains_in_{table}'
            connection.execute(f"""
            CREATE TABLE {agg_table}({columns});
            """)
            month_selection = order_month_selection(month_names=tables, table_name=table)
            connection.execute(f"""
            INSERT INTO {agg_table}
            SELECT {DOMAIN_AGGREGATION}
            {month_selection}
            FROM {table}
            WHERE domain_name IS NOT NULL
            GROUP BY domain_name;
            """)
            print(f'created_table: {agg_table}')
            print(duckdb.table(table_name=agg_table, connection=connection).describe())
            connection.execute(f"""
            DROP TABLE {table};
            """)
            progress.update(task_id=task_id, advance=1)


def sum_aggregates(connection, months):

    month_column_names = [f'nb_tweets_in_{month}' for _, month in months]

    base_domain_columns = ', '.join([f'{k} {v}' for k,v in DOMAIN_TABLE_DATA_TYPES.items()])
    month_columns = ', '.join([f'{column_name} UBIGINT' for column_name in month_column_names])
    columns = base_domain_columns+', '+month_columns

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
                    month_selection = ', '.join([f'SUM({month_column}) AS {month_column}' for month_column in month_column_names])
                    connection.execute(f"""
                    INSERT INTO {new_table_name}
                    SELECT  domain_id,
                            ANY_VALUE(domain_name),
                            SUM(nb_distinct_links_from_domain),
                            SUM(nb_collected_retweets_with_domain),
                            SUM(sum_all_tweets_with_domain),
                            SUM(nb_accounts_that_shared_domain_link),
                            {month_selection}
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
    connection.execute('PRAGMA enable_progress_bar')

    month_sum = '+'.join(month for month in month_column_names)
    median = f'{month_sum}) / {len(months)}'
    month_count = '+'.join([f'sign({month})' for month in month_column_names])
    generated_columns = f"""
    nb_collected_original_tweets_with_domain UBIGINT AS (sum_all_tweets_with_domain - nb_collected_retweets_with_domain) VIRTUAL,
    median_of_tweets_per_month FLOAT AS (round( ({median}, 2)) VIRTUAL,
    nb_months_with_tweet INTEGER AS ({month_count}) VIRTUAL
    """
    columns = columns+', '+generated_columns

    connection.execute(f"""
    CREATE TABLE {DOMAIN_TABLE}({columns});
    """)
    connection.execute(f"""
    INSERT INTO {DOMAIN_TABLE}
    SELECT *
    FROM {tables[0]}
    ORDER BY nb_accounts_that_shared_domain_link DESC;
    """)


def order_month_selection(month_names, table_name):
    value = f'COUNT(DISTINCT tweet_id) AS nb_tweets_in{table_name}'
    selection = []
    for month in month_names:
        if table_name == month:
            selection.append(value)
        else:
            selection.append('0')
    return ', '.join(selection)
