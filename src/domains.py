import duckdb
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from exceptions import MissingTable
from utilities import (
    build_aggregate_command_for_month_columns,
    create_month_column_names,
    extract_month,
    list_tables,
)


class MonthlyTweetData:
    def __init__(self, monthly_tweet_links_table_name: str) -> None:
        self.tweet_links_table_name = monthly_tweet_links_table_name
        self.month_name = extract_month(self.tweet_links_table_name)
        self.aggregated_domains_table_name = f"domains_in_{self.month_name}"


def aggregate_domains(connection: duckdb.DuckDBPyConnection, color: str):
    """Function to group every monthly table's tweets by domain name and sum the other metrics."""

    # Before continuing with this process, remove any existing monthly aggregate tables
    all_tables = connection.execute("SHOW TABLES;").fetchall()
    aggregate_tables = sorted(list_tables(all_tables=all_tables, prefix="domains"))
    if len(aggregate_tables) > 0:
        for table in aggregate_tables:
            query = f"""
            DROP TABLE {table};
            """
            connection.execute(query)

    # Extract a list of the database's monthly tweet links tables, each of whose link data will be grouped by domain
    monthly_tweet_data = [
        MonthlyTweetData(table[0])
        for table in all_tables
        if table[0].startswith("tweets_from")
    ]

    # So that each monthly domain aggregate table has a column for every month present in the dataset,
    # get all the monthly tweet links tables' names and extract the month part
    months_in_all_tweet_data = [m.month_name for m in monthly_tweet_data]
    month_column_names = create_month_column_names(months_in_all_tweet_data)
    month_column_names_and_data_types = ", ".join(
        [f"{column_name} UBIGINT" for column_name in month_column_names]
    )

    # ----------------------------------------------------------------------- #
    # Set up the progress bar
    ProgressCompleteColumn = Progress(
        TextColumn("{task.description}"),
        MofNCompleteColumn(),
        BarColumn(bar_width=60),
        TimeElapsedColumn(),
        expand=True,
    )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task(
            description=f"{color}Creating domain tables...", start=False
        )
        task2 = progress.add_task(
            description=f"{color}Aggregating domains in each table...", start=False
        )
        total = len(monthly_tweet_data)
        # ------------------------------------------------------------------ #

        # For every table of monthly twitter link data: (1) create a table for domain aggregates
        # and (2) insert the month's tweet data into that table while grouping by the domain
        for m in monthly_tweet_data:
            # Prepare the progress bar for task 1: Creating the table of domain aggregates
            progress.update(task_id=task1, total=total)
            progress.start_task(task_id=task1)

            # While adding however many month columns are necessary for the dataset, create the table
            query = f"""
            DROP TABLE IF EXISTS {m.aggregated_domains_table_name};
            CREATE TABLE {m.aggregated_domains_table_name}(
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

            # Prepare the progress bar for task 2: Inserting data into the domain aggregates table
            progress.update(task_id=task2, total=total)
            progress.start_task(task_id=task2)

            # While minding the order of the table's month columns, define the SQL command that
            # will sum this month's data into the relevant month column
            month_column_aggregate_string = build_aggregate_command_for_month_columns(
                month_column_names, m.month_name
            )
            query = f"""
            INSERT INTO {m.aggregated_domains_table_name}
            SELECT  domain_id,
                    ANY_VALUE(domain_name),
                    COUNT(DISTINCT normalized_url),
                    COUNT(DISTINCT retweeted_id),
                    COUNT(DISTINCT tweet_id),
                    COUNT(DISTINCT user_id),
                    {month_column_aggregate_string}
            FROM {m.tweet_links_table_name}
            WHERE domain_name IS NOT NULL
            GROUP BY domain_id;
            """
            connection.execute(query)
            progress.update(task_id=task2, advance=1)


def export_domains(connection: duckdb.DuckDBPyConnection, outfile: str):
    """Function to clean up after aggregation of domain names and to export result."""

    # If more than 1 table exists with the prefix "domains", the recursive aggregation of target tables failed
    all_tables = connection.execute("SHOW TABLES;").fetchall()
    domain_tables = sorted(list_tables(all_tables=all_tables, prefix="domains"))
    if not len(domain_tables) == 1:
        raise MissingTable
    sole_remaining_domain_table = domain_tables[0]

    # Create a table for the finalized domain data with a generated column that counts original tweets
    columns = duckdb.table(sole_remaining_domain_table, connection).columns
    data_types = duckdb.table(sole_remaining_domain_table, connection).dtypes
    columns_and_data_types = [f"{i[0]} {i[1]}" for i in list(zip(columns, data_types))]
    query = f"""
    DROP TABLE IF EXISTS all_domains;
    CREATE TABLE all_domains(
        {', '.join(columns_and_data_types)},
        nb_collected_original_tweets UBIGINT AS (sum_all_tweets_with_domain - nb_collected_retweets_with_domain) VIRTUAL
    );
    """
    connection.execute(query)

    # Insert the summed, aggregated domain data into the new table with its generated column
    query = f"""
    INSERT INTO all_domains
    SELECT {', '.join(columns)}
    FROM {sole_remaining_domain_table}
    ORDER BY sum_all_tweets_with_domain DESC;
    """
    connection.execute(query)

    # Having copied its contents to the final domain table, drop the old result of the recursive aggregation of previous domain tables
    query = f"""
    DROP TABLE {sole_remaining_domain_table};
    """
    connection.execute(query)

    # Export the final domain table to an out-file
    query = f"""
    COPY (SELECT * FROM all_domains) TO '{outfile}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)
