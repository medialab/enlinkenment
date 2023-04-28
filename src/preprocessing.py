from pathlib import Path

import duckdb
import polars
import pyarrow
import pyarrow.csv
import pyarrow.parquet
import ural
import ural.youtube
from rich.progress import (SpinnerColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from utilities import FileNaming, get_filepaths

# Configurations for selecting columns from CSV
SELECT_COLUMNS = [
        'id',
        'local_time',
        'user_id',
        'retweeted_id',
        'links'
    ]

def configure_pyarrow(columns):
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = columns
    convert_options.null_values = ['0']
    parser_options = pyarrow.csv.ParseOptions()
    parser_options.newlines_in_values = True
    return convert_options, parser_options

# Configurations for parsing links
UNALTERED_COLUMNS = [
    'id',
    'local_time',
    'user_id',
    'retweeted_id',
]

# Prefix for pre-resolution CSV files
PARSED_URL_PREFIX = 'parsed_urls'
PARSED_URL_FILE_PATTERN = PARSED_URL_PREFIX+'*.parquet'

# Columns for final pre-processing
FINAL_PREPROCESSING_COLUMNS = UNALTERED_COLUMNS+['link', 'domain']


def select_columns(infile:Path, outfile:Path, columns:list=SELECT_COLUMNS):
    infile_path_name = str(infile)
    convert_options, parser_options = configure_pyarrow(columns)
    writer = None
    with pyarrow.csv.open_csv(
        infile_path_name,
        convert_options=convert_options,
        parse_options=parser_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pyarrow.parquet.ParquetWriter(outfile, next_chunk.schema)
            next_table = pyarrow.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()


def deconcatenate_links(infile:Path):
    unaltered_columns = ', '.join(UNALTERED_COLUMNS)
    query = f"""
    SELECT {unaltered_columns}, UNNEST(links_list) AS link
    FROM (
        SELECT *, STRING_SPLIT(p.links, '|') as links_list
        FROM read_parquet('{str(infile)}') p
        WHERE LEN(links) > 1
    );
    """
    return duckdb.from_query(query).pl()


def attribute_domain(link:str):
    normalized_url = ural.normalize_url(link)
    try:
        domain = ural.get_domain_name(normalized_url)
    except Exception:
        domain = None
    else:
        if domain in ural.youtube.YOUTUBE_DOMAINS:
            domain = 'youtube.com'
    return domain


def parse_links(in_dataframe:polars.DataFrame, outfile):
    in_dataframe.with_columns([
        polars.col('link').apply(attribute_domain).\
            alias('domain'),
        polars.col('link').apply(ural.normalize_url).\
            alias('normalized_url')
    ]).write_parquet(file=outfile, compression='gzip')


def parse_input(input_data_path:str, input_file_pattern:str, output_dir:Path, color:str):
    files = get_filepaths(input_data_path, input_file_pattern)
    with Progress(
        TextColumn('{task.description}'),
        SpinnerColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn()
    ) as progress:
        total = total=len(files)
        file_task = progress.add_task(description=f'{color}Processing files...', total=total, start=True)
        for n, infile in enumerate(files):
            name_file = FileNaming(output_dir, infile)

            step1 = progress.add_task(description=f"[yellow]    Step 1. select columns...", total=total, start=False)
            progress.update(task_id=step1, completed=n)
            step2 = progress.add_task(description=f"[yellow]    Step 2. deconcatenate links...", total=total, start=False)
            progress.update(task_id=step2, completed=n)
            step3 = progress.add_task(description=f"[yellow]    Step 3. parse links...", total=total, start=False)
            progress.update(task_id=step3, completed=n)


            # Select relevant columns from CSV file
            task = step1
            progress.start_task(task_id=task)
            selected_columns_outfile = name_file.parquet('selected_columns')
            select_columns(infile, selected_columns_outfile)
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n+1)

            # De-concatenate URLs in "links" column
            task = step2
            progress.start_task(task_id=task)
            deconcatenate_links_dataframe = deconcatenate_links(selected_columns_outfile)
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n+1)

            # Parse links
            task = step3
            progress.start_task(task_id=task)
            parsed_urls_outfile = name_file.parquet(PARSED_URL_PREFIX)
            parse_links(deconcatenate_links_dataframe, parsed_urls_outfile)
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n+1)

            progress.update(task_id=file_task, advance=1)
            progress.remove_task(task_id=step1)
            progress.remove_task(task_id=step2)
            progress.remove_task(task_id=step3)
