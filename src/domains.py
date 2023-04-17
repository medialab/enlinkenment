import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)
from utilities import (create_month_column_names, extract_month,
                       fill_out_month_columns, list_tables)
from aggregate import sum_aggregated_tables

def aggregate_domains(connection:duckdb, color:str):

    # Get a list of all tables in the database
    all_tables = connection.execute('SHOW TABLES;').fetchall()
    aggregate_tables = sorted(list_tables(all_tables=all_tables, prefix='domains'))
    if len(aggregate_tables) > 0:
        for table in aggregate_tables:
            query = f"""
            DROP TABLE {table};
            """
            connection.execute(query)

    # Extract a list of only tables with months' tweet links
    month_tables = [
        (
            table[0],
            f'domains_in_{extract_month(table[0])}',
            extract_month(table[0])
        )
        for table in all_tables 
        if table[0].startswith('tweets_from')
    ]

    # Extract list of months
    months = [i[2] for i in month_tables]
    month_column_names = create_month_column_names(months)
    month_column_names_and_data_types = ', '.join(
        [f'{column_name} UBIGINT' for column_name in month_column_names]
    )

    # Set up the progress bar
    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task(description=f'{color}Creating domain tables...', start=False)
        task2 = progress.add_task(description=f'{color}Aggregating domains in each table...', start=False)

        # For every month of tweet data, create a table for domain aggregates
        total = len(month_tables)
        progress.update(task_id=task1, total=total)
        for tweet_table, domain_table, month_str in month_tables:
            progress.start_task(task_id=task1)
            # Create a domain aggregates table for the month
            query = f"""
            DROP TABLE IF EXISTS {domain_table};
            CREATE TABLE {domain_table}(
                domain_id VARCHAR,
                domain_name VARCHAR,
                nb_distinct_links_from_domain UBIGINT,
                nb_collected_retweets_with_domain UBIGINT,
                sum_all_tweets_with_domain UBIGINT,
                nb_accounts_that_shared_domain_link UBIGINT,
                {month_column_names_and_data_types}
                );
            """
            connection.execute(query)
            progress.update(task_id=task1, advance=1)

            progress.update(task_id=task2, total=total)
            progress.start_task(task_id=task2)
            # Make new 
            month_column_string = fill_out_month_columns(month_column_names, month_str)
            query = f"""
            INSERT INTO {domain_table}
            SELECT  domain_id,
                    ANY_VALUE(domain_name),
                    COUNT(DISTINCT normalized_url),
                    COUNT(DISTINCT retweeted_id),
                    COUNT(DISTINCT tweet_id),
                    COUNT(DISTINCT user_id),
                    {month_column_string}
            FROM {tweet_table}
            WHERE domain_name IS NOT NULL
            GROUP BY domain_id;
            """
            connection.execute(query)
            progress.update(task_id=task2, advance=1)


def sum_aggregated_domains(connection:duckdb, color:str):
    sum_aggregated_tables(
        connection=connection,
        targeted_table_prefix='domains_in',
        group_by=['domain_id', 'domain_name'],
        message='Summing aggregates of domains',
        color=color
    )


def export_domains(connection:duckdb, outfile:str):
    all_tables = connection.execute('SHOW TABLES;').fetchall()
    domain_tables = sorted(list_tables(all_tables=all_tables, prefix='domains'))
    if not len(domain_tables) == 1:
        raise RuntimeError
    final_domain_summed_table = domain_tables[0]
    columns = duckdb.table(final_domain_summed_table, connection).columns
    data_types = duckdb.table(final_domain_summed_table, connection).dtypes
    columns_and_data_types = [
        f'{i[0]} {i[1]}' for i in
        list(zip(columns, data_types))
        ]

    # Create a table for the finalized domain data with an
    # additional, generated column that counts original tweets
    query = f"""
    DROP TABLE IF EXISTS all_domains;
    CREATE TABLE all_domains(
        {', '.join(columns_and_data_types)},
        nb_collected_original_tweets UBIGINT AS (sum_all_tweets_with_domain - nb_collected_retweets_with_domain) VIRTUAL
    );
    """
    connection.execute(query)

    # Insert the summed, aggregated domain data
    query = f"""
    INSERT INTO all_domains
    SELECT {', '.join(columns)}
    FROM {final_domain_summed_table}
    ORDER BY sum_all_tweets_with_domain DESC;
    """
    connection.execute(query)

    # Remove the old summed, aggregated domain table
    query=f"""
    DROP TABLE {final_domain_summed_table};
    """
    connection.execute(query)

    query = f"""
    COPY (SELECT * FROM all_domains) TO '{outfile}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)
