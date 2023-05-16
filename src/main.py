import json
import shutil
import sys
from pathlib import Path

import click
import duckdb
from ebbe import Timer

from aggregate import aggregate_tables, recursively_aggregate_tables
from domains import domain_aggregate_sql, export_domains
from import_data import import_youtube_parsed_data, insert_processed_data
from preprocessing import PARSED_URL_FILE_PATTERN, parse_input
from utilities import SwitchColor
from youtube_channels import aggregate_channels
from youtube_links import (
    export_youtube_links,
    parse_youtube_links,
    youtube_link_aggregate_sql,
)
from youtube_videos import call_youtube_videos


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
    youtube_keys = None
    if config_file:
        with open(config_file, "r") as f:
            config = json.load(fp=f)
            youtube_keys = config["youtube"]["key_list"]
    elif key:
        youtube_keys = list(key)

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
            name="---->total time to pre-process data",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            parse_input(
                input_data_path=data_path,
                input_file_pattern=glob_file_pattern,
                output_dir=preprocessing_directory_path,
                color=color.set(),
            )
        print("")
    if skip_pre_processing and not preprocessing_directory_path.exists():
        raise FileNotFoundError

    # ------------------------------------------------------------------------ #
    # Step 2. Import the parsed URL twitter data into the database

    db_connection = duckdb.connect(str(database_path), read_only=False)

    with Timer(
        name="---->total time to import pre-processed data",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        insert_processed_data(
            connection=db_connection,
            preprocessing_dir=preprocessing_directory_path,
            input_file_pattern=PARSED_URL_FILE_PATTERN,
            color=color.set(),
        )
        print("")

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
    print("")

    with Timer(
        name="---->total time to sum all aggregated domains",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        recursively_aggregate_tables(
            connection=db_connection,
            targeted_table_prefix="domains_in",
            group_by=["domain_id", "domain_name"],
            color=color.set(),
            any_value=[],
        )
    print("")

    with Timer(
        name="---->total time to export aggregated domains",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        outfile_path_obj = output_directory_path.joinpath("domains.csv")
        export_domains(connection=db_connection, outfile=str(outfile_path_obj))

    # ------------------------------------------------------------------------ #
    # Step 4. Group together all the YouTube links

    youtube_dir = output_directory_path.joinpath("youtube")
    shutil.rmtree(youtube_dir, ignore_errors=True)
    youtube_dir.mkdir()
    youtube_links_path_obj = youtube_dir.joinpath("youtube_links.csv")
    youtube_parsed_channel_ids_path_obj = youtube_dir.joinpath(
        "youtube_channel_ids.csv"
    )
    youtube_videos_path_obj = youtube_dir.joinpath("youtube_videos.csv")
    youtube_videos_metadata_path_obj = youtube_dir.joinpath(
        "youtube_video_metadata.csv"
    )
    aggregated_youtube_channels_path_obj = youtube_dir.joinpath(
        "aggregated_youtube_channels.csv"
    )

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
    print("")

    with Timer(
        name="---->total time to sum all aggregated YouTube links",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        recursively_aggregate_tables(
            connection=db_connection,
            targeted_table_prefix="youtube_links",
            group_by=["normalized_url"],
            any_value=["link_for_scraping"],
            color=color.set(),
        )
    print("")

    with Timer(
        name="---->total time to export aggregated YouTube links",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        export_youtube_links(
            connection=db_connection, outfile=str(youtube_links_path_obj)
        )
    print("")

    with Timer(
        name="---->total time to get every links' channel ID",
        file=sys.stdout,
        precision="nanoseconds",
    ):
        parse_youtube_links(
            infile=youtube_links_path_obj,
            channel_outfile=youtube_parsed_channel_ids_path_obj,
            video_outfile=youtube_videos_path_obj,
        )

    # ------------------------------------------------------------------------ #
    # Step 4. Get channel data

    if youtube_keys:
        with Timer(
            name="---->total time to parse YouTube links",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            call_youtube_videos(
                infile=youtube_videos_path_obj,
                outfile=youtube_videos_metadata_path_obj,
                keys=youtube_keys,
            )

        with Timer(
            name="---->total time to import parsed YouTube link data",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            import_youtube_parsed_data(
                connection=db_connection,
                video_infile=youtube_videos_metadata_path_obj,
                channel_infile=youtube_parsed_channel_ids_path_obj,
            )
        print("")

        with Timer(
            name="---->total time to aggregate YouTube channels",
            file=sys.stdout,
            precision="nanoseconds",
        ):
            aggregate_channels(
                connection=db_connection,
                outfile=aggregated_youtube_channels_path_obj,
            )
        print("")


if __name__ == "__main__":
    main()
