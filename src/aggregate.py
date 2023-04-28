import duckdb
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from utilities import extract_month, list_tables, pair_tables


def sum_aggregated_tables(
    connection:duckdb,
    targeted_table_prefix:str,
    group_by:list,
    message:str,
    color:str
):
    # Get a list of all tables in the database
    all_tables = connection.execute('SHOW TABLES;').fetchall()

    # Extract a list of only tables with months' tweet links
    aggregate_tables = sorted(list_tables(all_tables=all_tables, prefix=targeted_table_prefix))

    if len(aggregate_tables)//2 == len(aggregate_tables)/2:
        total_tours = len(aggregate_tables)//2
    else:
        total_tours = len(aggregate_tables)//2 + len(aggregate_tables)%2

    # Set up the progress bar
    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task(description=f'{color}{message}...', start=True, total=total_tours)

        # While more than 1 domain aggregate table exists,
        # continue pairing up the tables and summing their values
        while len(aggregate_tables) > 1:
            paired_tables = pair_tables(aggregate_tables)

            # For every pair, combine the tables then sum the aggregated values
            for pair in paired_tables:
                if len(pair) == 2:
                    left_table = pair[0]
                    right_table = pair[1]
                    new_table_name = '{}_{}_{}'.format(
                        targeted_table_prefix,
                        extract_month(left_table),
                        extract_month(right_table)
                    )

                    # Get column names for the new table
                    columns = duckdb.table(left_table, connection).columns
                    data_types = duckdb.table(left_table, connection).dtypes
                    columns_and_data_types = [
                        f'{i[0]} {i[1]}' for i in
                        list(zip(columns, data_types))
                        ]

                    # Create the new table for the summed pairs
                    query = f"""
                    DROP TABLE IF EXISTS {new_table_name};
                    CREATE TABLE {new_table_name}(
                        {', '.join(columns_and_data_types)}
                    )
                    """
                    connection.execute(query)

                    # Combine the two paired tables
                    query = f"""
                    INSERT INTO {left_table}
                    SELECT *
                    FROM {right_table};
                    """
                    connection.execute(query)

                    # Group the combined table by domain
                    for col in group_by:
                        columns.remove(col)
                    summed_columns = [
                        f'SUM({col})' for col in columns
                    ]
                    query = f"""
                    INSERT INTO {new_table_name}
                    SELECT  {', '.join(group_by)},
                            {', '.join(summed_columns)}
                    FROM {left_table}
                    GROUP BY ({', '.join(group_by)});
                    """
                    connection.execute(query)

                    # Now that their data has been inserted into the
                    # combined table, remove the paired tables
                    query = f"""
                    DROP TABLE {left_table};
                    DROP TABLE {right_table};
                    """
                    connection.execute(query)

            progress.update(task_id=task1, advance=1)

            # Recalculate how many domain aggregate tables remain
            all_tables = connection.execute('SHOW TABLES;').fetchall()
            aggregate_tables = sorted(list_tables(all_tables=all_tables, prefix=targeted_table_prefix))