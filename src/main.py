import shutil
from pathlib import Path

import click
import duckdb
from domains import (aggregate_domains, export_domains,
                               sum_aggregated_domains)
from youtube_channels import aggregate_youtube_links, sum_aggregated_youtube_links, request_youtube_channel_data
from import_data import insert_processed_data
from preprocessing import (POST_RESOLUTION_FILE_PATTERN,
                           PRE_RESOLUTION_FILE_PATTERN, normalize_final_urls,
                           parse_input, resolve_youtube_urls)


@click.command()
@click.argument('data')
@click.option('-f', '--glob-file-pattern', type=click.types.STRING, default='**/*.gz', show_default=True)
@click.option('-d', '--database-name', type=click.types.STRING, default='twitter_links', show_default=True)
@click.option('-o', '--output-dir', type=click.types.STRING, default='output', show_default=True)
@click.option('-k', '--youtube-key', type=click.types.STRING)
@click.option('--skip-preprocessing', is_flag=True, show_default=False, default=False)
def main(data, glob_file_pattern, database_name, output_dir, youtube_key, skip_preprocessing):

    # --------------------------------- #
    #         PRE-PROCESS DATA
    output_directory_path = Path(output_dir)
    end_of_preprocessing_file_pattern = 'finalized*'

    # Determine whether to proceed with preprocessing below
    if not skip_preprocessing \
        or not output_directory_path.is_dir() \
            or not True in [
                file.match(end_of_preprocessing_file_pattern)
                for file in output_directory_path.iterdir()
            ]:

        shutil.rmtree(output_dir, ignore_errors=True)
        output_directory_path.mkdir(exist_ok=True)
        prep_directory_path = output_directory_path.joinpath('prep')
        prep_directory_path.mkdir()

        parse_input(
            input_data_path=data,
            input_file_pattern=glob_file_pattern,
            output_dir=prep_directory_path
        )

        resolve_youtube_urls(
            input_file_pattern=PRE_RESOLUTION_FILE_PATTERN,
            output_dir=prep_directory_path
        )

        normalize_final_urls(
            input_dir=prep_directory_path,
            input_file_pattern=POST_RESOLUTION_FILE_PATTERN,
            output_dir=output_directory_path,
        )

    # --------------------------------- #
    #         IMPORT DATA

    database_path = output_directory_path.joinpath(f'{database_name}.duckdb')

    db_connection = duckdb.connect(str(database_path), read_only=False)

    insert_processed_data(
        connection=db_connection,
        input_dir=output_directory_path,
        input_file_pattern='*.parquet'
    )

    # --------------------------------- #
    #         AGGREGATE DOMAINS

    aggregate_domains(
        connection=db_connection
    )

    sum_aggregated_domains(
        connection=db_connection
    )

    outfile_path_obj = output_directory_path.joinpath('domains.csv')
    export_domains(
        connection=db_connection,
        outfile=str(outfile_path_obj)
    )

    # --------------------------------- #
    #     AGGREGATE YOUTUBE CHANNELS

    youtube_dir = output_directory_path.joinpath('youtube')
    shutil.rmtree(youtube_dir, ignore_errors=True)
    youtube_dir.mkdir()

    aggregate_youtube_links(
        connection=db_connection,
    )

    sum_aggregated_youtube_links(
        connection=db_connection
    )

    request_youtube_channel_data(
        output_dir=youtube_dir,
        key=youtube_key,
        connection=db_connection
    )

if __name__ == "__main__":
    main()
