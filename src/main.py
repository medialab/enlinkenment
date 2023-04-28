import shutil
import sys
from pathlib import Path
import json

import click
import duckdb
from ebbe import Timer

from domains import aggregate_domains, export_domains, sum_aggregated_domains
from import_data import insert_processed_data
from preprocessing import PARSED_URL_FILE_PATTERN, parse_input
from youtube_channels import (aggregate_youtube_links,
                              request_youtube_channel_data,
                              sum_aggregated_youtube_links, aggregate_youtube_channel_data)


@click.command()
@click.argument('data')
@click.option('-f', '--glob-file-pattern', type=click.types.STRING, default='**/*.gz', show_default=True)
@click.option('-k', '--key', multiple=True, required=False)
@click.option('-c', '--config-file', required=False)
@click.option('--skip-preprocessing', is_flag=True, show_default=False, default=False)
def main(data, glob_file_pattern, key, config_file, skip_preprocessing):

    will_get_youtube_data = False
    if config_file:
        with open(config_file, 'r') as f:
            config = json.load(fp=f)
            will_get_youtube_data = True
    elif key:
        key_list = list(key)
        config = {'youtube':{'keys':key_list}}
        will_get_youtube_data = True

    # --------------------------------- #
    #         PRE-PROCESS DATA
    output_dir = 'output'
    output_directory_path = Path(output_dir)

    # Determine whether to proceed with preprocessing below
    if not skip_preprocessing \
        or not output_directory_path.is_dir() \
            or not True in [
                file.match(PARSED_URL_FILE_PATTERN)
                for file in output_directory_path.iterdir()
            ]:

        shutil.rmtree(output_dir, ignore_errors=True)
        output_directory_path.mkdir(exist_ok=True)
        prep_directory_path = output_directory_path.joinpath('prep')
        prep_directory_path.mkdir()

        with Timer(name='---->total time to parse input', file=sys.stdout, precision='nanoseconds'):
            parse_input(
                input_data_path=data,
                input_file_pattern=glob_file_pattern,
                output_dir=prep_directory_path,
                color='[bold green]'
            )

    # --------------------------------- #
    #         IMPORT DATA

    database_name = 'twitter_links.duckdb'
    database_path = output_directory_path.joinpath(f'{database_name}.duckdb')

    db_connection = duckdb.connect(str(database_path), read_only=False)

    with Timer(name='---->total time to insert preprocessed data', file=sys.stdout, precision='nanoseconds'):
        insert_processed_data(
            connection=db_connection,
            input_dir=prep_directory_path,
            input_file_pattern=PARSED_URL_FILE_PATTERN,
            color='[bold blue]'
        )

    # --------------------------------- #
    #         AGGREGATE DOMAINS

    with Timer(name='---->total time to aggregate domains', file=sys.stdout, precision='nanoseconds'):
        aggregate_domains(
            connection=db_connection,
            color='[bold green]'
        )

    with Timer(name='---->total time to sum aggregated domains', file=sys.stdout, precision='nanoseconds'):
        sum_aggregated_domains(
            connection=db_connection,
            color='[bold blue]'
        )

    with Timer(name='---->total time to export domains', file=sys.stdout, precision='nanoseconds'):
        outfile_path_obj = output_directory_path.joinpath('domains.csv')
        export_domains(
            connection=db_connection,
            outfile=str(outfile_path_obj)
        )

    # --------------------------------- #
    #     AGGREGATE YOUTUBE CHANNELS

    if will_get_youtube_data:

        youtube_dir = output_directory_path.joinpath('youtube')
        shutil.rmtree(youtube_dir, ignore_errors=True)
        youtube_dir.mkdir()

        with Timer(name='---->total time to aggregate YouTube URLs', file=sys.stdout, precision='nanoseconds'):
            aggregate_youtube_links(
                connection=db_connection,
                color='[bold green]'
            )

        with Timer(name='---->total time to sum aggregated YouTube URLs', file=sys.stdout, precision='nanoseconds'):
            sum_aggregated_youtube_links(
                connection=db_connection,
                color='[bold blue]'
            )

        with Timer(name='---->total time to request YouTube data', file=sys.stdout, precision='nanoseconds'):
            request_youtube_channel_data(
                output_dir=youtube_dir,
                config=config,
                connection=db_connection,
                color='[bold green]'
            )

        # with Timer(name='---->total time to aggregate YouTube channel data', file=sys.stdout, precision='nanoseconds'):
        #     aggregate_youtube_channel_data(
        #         output_dir=youtube_dir,
        #         connection=db_connection
        #     )

if __name__ == "__main__":
    main()
