import math

import duckdb
from rich.align import Align
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from utilities import (
    MonthlyTweetData,
    build_aggregate_command_for_month_columns,
    create_month_column_names,
    extract_month,
    list_tables,
    pair_tables,
)


class AggregateSQL:
    def __init__(
        self,
        new_table_constant_columns: list[str],
        select: str,
        where: str,
        group_by: str,
    ) -> None:
        self.columns = ", ".join(new_table_constant_columns)
        if select.rstrip()[-1] != ",":
            self.select = select + ","
        else:
            self.select = select
        self.where = where
        self.group_by = group_by


def aggregate_tables(
    connection: duckdb.DuckDBPyConnection,
    color: str,
    target_table_prefix: str,
    sql: AggregateSQL,
):
    """Function to aggregate every every target table's tweets according to the "group_by" column given in the sql parameter."""

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
        MonthlyTweetData(table[0], target_table_prefix)
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
            description=f"{color}Creating monthly aggregate tables...", start=False
        )
        task2 = progress.add_task(
            description=f"{color}Aggregating data in each table...", start=False
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
            DROP TABLE IF EXISTS {m.aggregated_table_name};
            CREATE TABLE {m.aggregated_table_name}(
                {sql.columns},
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
            INSERT INTO {m.aggregated_table_name}
            SELECT  {sql.select}
                    {month_column_aggregate_string}
            FROM {m.tweet_links_table_name}
            WHERE domain_name IS NOT NULL
            GROUP BY {sql.group_by};
            """
            connection.execute(query)
            progress.update(task_id=task2, advance=1)


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


def write_live_table_row(table: Table, tour: str, pairings: list):
    cells = []
    for pair in pairings:
        if len(pair) == 2:
            cells.append(f"[red]{pair[0]} [white]& [blue]{pair[1]}")
        else:
            cells.append(f"[green]{pair}")
    table.add_row(tour, *cells)
