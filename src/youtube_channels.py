from pathlib import Path
import duckdb


def aggregate_channels(connection: duckdb.DuckDBPyConnection, outfile: Path):
    """Function groups parsed YouTube links by the channel ID.

    Args:
        connection (duckdb.DuckDBPyConnection): connection to database
        outfile (Path): path to CSV file of aggregated YouTube channels
    """
    connection.execute("PRAGMA enable_progress_bar")

    # Get variables
    aggregate_table_name = "aggregated_youtube_channels"
    parsed_table_name = "all_parsed_youtube_links"
    parsed_table = duckdb.table(parsed_table_name, connection=connection)
    parsed_column_names = parsed_table.columns
    parsed_column_and_dtypes = ", ".join(
        [
            f"{col} {dtype}"
            for col, dtype in list(zip(parsed_column_names, parsed_table.dtypes))
        ]
    )

    # Create new table to store aggregation
    query = f"""
    DROP TABLE IF EXISTS {aggregate_table_name};
    CREATE TABLE {aggregate_table_name}(
        {parsed_column_and_dtypes},
        nb_links UBIGINT
    );
    """
    connection.execute(query)

    # Get list of columns for summed aggregates
    columns_to_sum = parsed_column_names
    columns_to_sum.remove("channel_id")
    columns_to_sum.remove("normalized_url")
    columns_to_sum.remove("link_for_scraping")
    columns_to_sum = ", ".join(
        [f"SUM(CAST({col} AS UBIGINT))" for col in columns_to_sum]
    )

    # Group by YouTube channel ID and sum aggregated metrics
    query = f"""
    INSERT INTO {aggregate_table_name}
    SELECT  ANY_VALUE(normalized_url) AS normalized_url,
            ANY_VALUE(link_for_scraping) AS link_for_scraping,
            {columns_to_sum},
            channel_id AS channel_id,
            COUNT(channel_id)
    FROM {parsed_table_name}
    WHERE channel_id IS NOT NULL
    GROUP BY channel_id;
    """
    connection.execute(query)

    # Export the final domain table to an out-file
    query = f"""
    COPY (SELECT * FROM {aggregate_table_name}) TO '{str(outfile)}' (HEADER, DELIMITER ',');
    """
    connection.execute(query)
