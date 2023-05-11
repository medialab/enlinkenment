import math

import duckdb
from rich.live import Live
from rich.table import Table
from rich.align import Align

from utilities import extract_month, list_tables, pair_tables


def write_live_table_row(table: Table, tour: str, pairings: list):
    cells = []
    for pair in pairings:
        if len(pair) == 2:
            cells.append(f"[red]{pair[0]} [white]& [blue]{pair[1]}")
        else:
            cells.append(f"[green]{pair}")
    table.add_row(tour, *cells)


def recursively_aggregate_tables(
    connection: duckdb.DuckDBPyConnection,
    targeted_table_prefix: str,
    group_by: list,
):
    """Function to recursively concatenate pairs of tables and re-aggregate their contents until no more pairs can be made and all the targeted tables have been combined into one."""

    # Based on a consistent prefix, list the tables to recurisvely aggregate
    all_tables = connection.execute("SHOW TABLES;").fetchall()
    target_tables = sorted(
        list_tables(all_tables=all_tables, prefix=targeted_table_prefix)
    )

    # Calculate the number of times it will take to recursively pair off the tables
    total_tours = round(math.log(len(target_tables), 2))

    # ----------------------------------------------------------------------- #
    # Set up the progress table
    print(
        f"\nRecursively concatenating pairs of tables and re-aggregating their contents until no more pairs can be made and all the targeted tables have been combined into one."
    )
    table = Table()
    table_centered = Align.center(table)
    table.add_column("Tour", no_wrap=False)
    [table.add_column("Tables", no_wrap=False) for _ in range(total_tours)]
    with Live(table_centered, refresh_per_second=4):
        # ----------------------------------------------------------------------- #

        tour = 0
        # While at least 2 target tables still exist, continue pairing up the tables
        while len(target_tables) > 1:
            tour += 1
            # At the start of the loop, redo the pairing of remaining target tables
            table_pairings = pair_tables(target_tables)
            write_live_table_row(table=table, tour=str(tour), pairings=table_pairings)

            for pair in table_pairings:
                # If the item in the list of table pairings is indeed 2 tables, and not an odd remaining table,
                # proceed to concatenate and aggregate the two tables
                if len(pair) == 2:
                    left_table = pair[0]
                    right_table = pair[1]
                    new_table_name = "{}_{}_{}".format(
                        targeted_table_prefix,
                        extract_month(left_table),
                        extract_month(right_table),
                    )

                    # Concatenate the two paired tables' contents
                    query = f"""
                    INSERT INTO {left_table}
                    SELECT *
                    FROM {right_table};
                    """
                    connection.execute(query)

                    # From one of the tables, extract the column names and their data types
                    columns = duckdb.table(left_table, connection).columns
                    data_types = duckdb.table(left_table, connection).dtypes
                    columns_and_data_types = [
                        f"{i[0]} {i[1]}" for i in list(zip(columns, data_types))
                    ]

                    # Using the copied column names and data types, create a new table into which a new
                    # aggregation of the paired tables will be inserted
                    query = f"""
                    DROP TABLE IF EXISTS {new_table_name};
                    CREATE TABLE {new_table_name}(
                        {', '.join(columns_and_data_types)}
                    )
                    """
                    connection.execute(query)

                    # After removing the target column by which the data will grouped, construct the SQL command
                    # that will sum the aggregates of the remaining columns
                    for col in group_by:
                        columns.remove(col)
                    summed_columns = [f"SUM({col})" for col in columns]

                    # On the concatenated data, group by the target column and insert into the combined table
                    query = f"""
                    INSERT INTO {new_table_name}
                    SELECT  {', '.join(group_by)},
                            {', '.join(summed_columns)}
                    FROM {left_table}
                    GROUP BY ({', '.join(group_by)});
                    """
                    connection.execute(query)

                    # Now that their data has been inserted into the combined table, drop the paired tables from the database
                    query = f"""
                    DROP TABLE {left_table};
                    DROP TABLE {right_table};
                    """
                    connection.execute(query)

            # Recalculate how many target tables remain
            all_tables = connection.execute("SHOW TABLES;").fetchall()
            target_tables = sorted(
                list_tables(all_tables=all_tables, prefix=targeted_table_prefix)
            )
