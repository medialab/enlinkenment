import json
import shutil
import sys
from pathlib import Path

import click
import duckdb
from ebbe import Timer

from aggregate import aggregate_tables, recursively_aggregate_tables
from domains import domain_aggregate_sql, export_domains
from import_data import insert_processed_data
from preprocessing import PARSED_URL_FILE_PATTERN, parse_input
from utilities import SwitchColor
from youtube_links import export_youtube_links, youtube_link_aggregate_sql


@click.command()
@click.option(
    "-d",
    "--data",
    type=click.types.STRING,
    help="The path to a CSV file or the path to a directory containing CSV files.",
)
@click.option(
    "-f",
    "--glob-file-pattern",
    type=click.types.STRING,
    default="**/*.gz",
    show_default=True,
    help='A pattern (i.e. "*.csv") that captures the files targeted for processing in the given directory.',
)
@click.option(
    "-k",
    "--key",
    multiple=True,
    required=False,
    help="A YouTube API key. This option may be given multiple times if multiple keys are available (i.e. -k KEY1 -k KEY2)",
)
@click.option(
    "-c",
    "--config-file",
    required=False,
    help=f"A JSON or YAML file that has an array of YouTube API keys (see example file).",
)
@click.option(
    "--skip-pre-processing",
    is_flag=True,
    show_default=False,
    default=False,
    help="This flag skips the steps of parsing the raw twitter data and moves directly to importing pre-processed parquet files into the database for aggregation and further processing.",
)
def main(data, glob_file_pattern, key, config_file, skip_pre_processing):
    data_path = Path(data)

    # If given, parse the array of youtube API keys
    config = None
    if config_file:
        with open(config_file, "r") as f:
            config = json.load(fp=f)
    elif key:
        key_list = list(key)
        config = {"youtube": {"key_list": key_list}}

    # Set up file paths for the output
    output_dir_name = "output"
    output_directory_path = Path(output_dir_name)
    preprocessing_directory_path = output_directory_path.joinpath("pre-processing")
    database_name = "twitter_links"
    database_path = output_directory_path.joinpath(f"{database_name}.duckdb")

    color = SwitchColor()

    # ------------------------------------------------------------------------ #
    # Step 1. Isolate and parse URLs from raw twitter data

    # If the directory "output/pre-processing/" doesn't already have each input
    # file's URLs de-concatenated and parsed with Ural, clear out the "output/"
    # directory and run parse_input() on the data file(s)
    if not skip_pre_processing:
        shutil.rmtree(output_dir_name, ignore_errors=True)
        output_directory_path.mkdir(exist_ok=True)
        preprocessing_directory_path.mkdir()

        with Timer(
            name="---->total time to parse input",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            parse_input(
                input_data_path=data_path,
                input_file_pattern=glob_file_pattern,
                output_dir=preprocessing_directory_path,
                color=color.set(),
            )
    if skip_pre_processing and not preprocessing_directory_path.exists():
        raise FileNotFoundError

    # ------------------------------------------------------------------------ #
    # Step 2. Import the parsed URL twitter data into the database

    db_connection = duckdb.connect(str(database_path), read_only=False)

    with Timer(
        name="---->total time to insert preprocessed data",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        insert_processed_data(
            connection=db_connection,
            preprocessing_dir=preprocessing_directory_path,
            input_file_pattern=PARSED_URL_FILE_PATTERN,
            color=color.set(),
        )

    # ------------------------------------------------------------------------ #
    # Step 3. Group the twitter data by the parsed domain name of each URL

    with Timer(
        name="---->total time to aggregate domains for each month",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        aggregate_tables(
            connection=db_connection,
            color=color.set(),
            target_table_prefix="domains_in",
            sql=domain_aggregate_sql(),
        )

    with Timer(
        name="---->total time to sum all aggregated domains",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        recursively_aggregate_tables(
            connection=db_connection,
            targeted_table_prefix="domains_in",
            group_by=["domain_id", "domain_name"],
        )

    with Timer(
        name="---->total time to export aggregated domains",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        outfile_path_obj = output_directory_path.joinpath("domains.csv")
        export_domains(connection=db_connection, outfile=str(outfile_path_obj))

    # ------------------------------------------------------------------------ #
    # Step 4. Group together all the YouTube links

    if config:
        youtube_dir = output_directory_path.joinpath("youtube")
        shutil.rmtree(youtube_dir, ignore_errors=True)
        youtube_dir.mkdir()

        with Timer(
            name="---->total time to aggregate YouTube links for each month",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            aggregate_tables(
                connection=db_connection,
                color=color.set(),
                target_table_prefix="youtube_links",
                sql=youtube_link_aggregate_sql(),
            )

        with Timer(
            name="---->total time to sum all aggregated YouTube links",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            recursively_aggregate_tables(
                connection=db_connection,
                targeted_table_prefix="youtube_links",
                group_by=["normalized_url"],
            )

        with Timer(
            name="---->total time to export aggregated YouTube links",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            outfile_path_obj = youtube_dir.joinpath("youtube_links.csv")
            export_youtube_links(
                connection=db_connection, outfile=str(outfile_path_obj)
            )


if __name__ == "__main__":
    main()
