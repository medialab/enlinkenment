from pathlib import Path

import casanova
import duckdb
import polars
import pyarrow
import pyarrow.csv
import pyarrow.parquet
import ural
import ural.youtube
from minet import multithreaded_resolve
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn, SpinnerColumn)

from utilities import FileNaming, get_filepaths

# Configurations for selecting columns from CSV
SELECT_COLUMNS = [
        'id',
        'local_time',
        'user_id',
        'retweeted_id',
        'links'
    ]
def configure_pyarrow():
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = SELECT_COLUMNS
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
PRE_RESOLUTION_PREFIX = 'pre_resolution'
PRE_RESOLUTION_FILE_PATTERN = PRE_RESOLUTION_PREFIX+'*.csv'

# Prefix for post-resolution CSV files
POST_RESOLUTION_PREFIX = 'resolved'
POST_RESOLUTION_FILE_PATTERN = POST_RESOLUTION_PREFIX+'*.csv'

# Prefix for files after getting YouTube metadata
POST_YOUTUBE_PREFIX = 'with_youtube'
POST_YOUTUBE_FILE_PATTERN = POST_YOUTUBE_PREFIX+'*.csv'

# Columns for final pre-processing
FINAL_PREPROCESSING_COLUMNS = UNALTERED_COLUMNS+['link', 'domain']


def select_columns(infile:Path, outfile:Path):
    infile_path_name = str(infile)
    convert_options, parser_options = configure_pyarrow()
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


def parse_links(in_dataframe:polars.DataFrame):
    return in_dataframe.with_columns([
        polars.col('link').apply(attribute_domain).\
            alias('domain'),
        polars.col('link').apply(ural.should_resolve).\
            alias('needs_resolved')
    ])


def add_column_for_yt_resolution(
    in_dataframe:polars.DataFrame,
    outfile:Path
):
    df = in_dataframe.with_columns([
        polars.when(
            polars.col('domain').is_in(['youtube.com']) &\
            polars.col('needs_resolved') == True
        ).then(polars.col('link')).otherwise('').\
            alias('youtube_url_to_resolve')
    ])
    df.write_csv(outfile)


def parse_input(input_data_path:str, input_file_pattern:str, output_dir:Path, color:str):
    files = get_filepaths(input_data_path, input_file_pattern)
    with Progress(
        TextColumn('{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn()
    ) as progress:
        total = total=len(files)
        task1 = progress.add_task(description=f'{color}Select columns...', total=total, start=False)
        task2 = progress.add_task(description=f'{color}Deconcatenate links...', total=total, start=False)
        task3 = progress.add_task(description=f'{color}Parse links...', total=total, start=False)
        task4 = progress.add_task(description=f'{color}Give YouTube link to resolve...', total=total, start=False)
        for infile in files:
            name_file = FileNaming(output_dir, infile)

            # Select relevant columns from CSV file
            progress.start_task(task_id=task1)
            selected_columns_outfile = name_file.parquet('selected_columns')
            select_columns(infile, selected_columns_outfile)
            progress.update(task_id=task1, advance=1)
            progress.stop_task(task_id=task1)

            # De-concatenate URLs in "links" column
            progress.start_task(task_id=task2)
            deconcatenate_links_dataframe = deconcatenate_links(selected_columns_outfile)
            progress.update(task_id=task2, advance=1)
            progress.stop_task(task_id=task2)

            # Parse links
            progress.start_task(task_id=task3)
            parsed_links_dataframe = parse_links(deconcatenate_links_dataframe)
            progress.update(task_id=task3, advance=1)
            progress.stop_task(task_id=task3)

            # Give YouTube link to resolve
            progress.start_task(task_id=task4)
            pre_resolution_outfile = name_file.csv(PRE_RESOLUTION_PREFIX)
            add_column_for_yt_resolution(
                in_dataframe=parsed_links_dataframe,
                outfile=pre_resolution_outfile
            )
            progress.update(task_id=task4, advance=1)
            progress.stop_task(task_id=task4)


def resolve_youtube_urls(output_dir:Path, input_file_pattern:str, color:str):
    files = get_filepaths(output_dir, input_file_pattern)
    with Progress(
        TextColumn('{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn()
    ) as progress:
        task1 = progress.add_task(f'{color}Multithreaded YouTube URL resolution...', start=False, total=len(files))
        task2 = progress.add_task(f'{color}    Progress through CSV...', start=False)
        progress.start_task(task_id=task1)
        for file in files:
            name_file = FileNaming(
                output_dir=output_dir,
                infile=file,
                remove_prefix=PRE_RESOLUTION_PREFIX
            )
            resolved_outfile_name = name_file.csv(prefix=POST_RESOLUTION_PREFIX)
            with open(file) as f, open(resolved_outfile_name, 'w') as of:
                total = casanova.reader.count(str(file))
                progress.reset(task_id=task2, completed=0, total=total, start=True)
                keep_columns = UNALTERED_COLUMNS+['link', 'domain', 'needs_resolved']
                enricher = casanova.enricher(f, of, add=['resolved_youtube_url'], keep=keep_columns)
                for result in multithreaded_resolve(enricher.cells('youtube_url_to_resolve', with_rows=True), key=lambda x:x[1]):
                    resolved_youtube_url = None
                    if result.stack and len(result.stack) > 0:
                        resolved_youtube_url = result.stack[-1].url
                    row = result.item[0]+[resolved_youtube_url]
                    enricher.writerow(row)
                    progress.update(task_id=task2, advance=1)
            progress.update(task_id=task1, advance=1)


def normalize_final_urls(input_dir:Path, output_dir:Path, input_file_pattern:str, color:str):
    files = get_filepaths(input_dir, input_file_pattern)
    with Progress(
        TextColumn(f'{color}URL normalization...'),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn()
    ) as progress:
        for file in progress.track(files, total=len(files)):
            name_file = FileNaming(
                output_dir=output_dir,
                infile=file,
                remove_prefix=POST_RESOLUTION_PREFIX
            )
            outfile_name = name_file.parquet('finalized')
            in_dataframe = polars.read_csv(
                file=file,
                has_header=True,
            )
            # Populate 'clean_url' column with either
            # (a) resolved YouTube URL or (b) unaltered link
            dataframe_with_clean_url = in_dataframe.with_columns([
                polars.when(
                    polars.col('resolved_youtube_url').is_not_null()
                ).then(
                    polars.col('resolved_youtube_url')
                ).otherwise(
                    polars.col('link')
                ).alias('clean_url')
            ])
            out_dataframe = dataframe_with_clean_url.with_columns(
                polars.col('clean_url').apply(ural.normalize_url).alias('normalized_url')
            ).drop(['resolved_youtube_url'])

            out_dataframe.write_parquet(outfile_name, compression='gzip')
