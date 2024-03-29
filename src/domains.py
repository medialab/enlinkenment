import duckdb

from aggregate import AggregateSQL
from exceptions import MissingTable
from utilities import list_tables


def domain_aggregate_sql() -> AggregateSQL:
    new_table_columns = [
        "domain_id VARCHAR",
        "domain_name VARCHAR",
        "nb_distinct_links_from_domain UBIGINT",
        "nb_collected_retweets_with_domain UBIGINT",
        "sum_all_tweets_with_domain UBIGINT",
        "nb_accounts_that_shared_domain_link UBIGINT",
    ]
    select = """
            domain_id,
            ANY_VALUE(domain_name),
            COUNT(DISTINCT normalized_url),
            COUNT(DISTINCT retweeted_id),
            COUNT(DISTINCT tweet_id),
            COUNT(DISTINCT user_id),
    """
    return AggregateSQL(
        new_table_constant_columns=new_table_columns,
        select=select,
        where="domain_name IS NOT NULL",
        group_by="domain_id",
    )


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
