import os
from pathlib import Path
import duckdb
import csv

import polars
import pyarrow
import pyarrow.csv
import pyarrow.parquet
from rich.progress import (Progress, SpinnerColumn, TextColumn,
                           TimeElapsedColumn)
from ural import get_domain_name as ural_get_domain_name
from ural import normalize_url as ural_normalize_url
from ural.youtube import YOUTUBE_DOMAINS

from CONSTANTS import MAINTABLENAME
from utils import Timer

# tweet column data fields
select_columns = [
    'id',
    'timestamp_utc',
    'local_time',
    'retweet_count',
    'like_count',
    'reply_count',
    'user_id',
    'user_followers',
    'user_friends',
    'retweeted_id',
    'retweeted_user_id',
    'quoted_id',
    'quoted_user_id',
    'links'
    ]

select_columns_without_links = ', '.join(select_columns[:-1])

columns = {
    'tweet_id':'VARCHAR',
    'timestamp_utc':'VARCHAR',
    'local_time':'DATETIME',
    'retweet_count':'VARCHAR',
    'like_count':'VARCHAR',
    'reply_count':'VARCHAR',
    'user_id':'VARCHAR',
    'user_followers':'VARCHAR',
    'user_friends':'VARCHAR',
    'retweeted_id':'VARCHAR',
    'retweeted_user_id':'VARCHAR',
    'quoted_id':'VARCHAR',
    'quoted_user_id':'VARCHAR',
    'link':'VARCHAR',
    'normalized_url':'VARCHAR',
    'domain_name':'VARCHAR'
    }


def parse_raw_data(datapath, file_pattern, connection):
    """Iterate through Tweet data files and import into a main table."""

    timer = Timer('Creating main table')
    column_datatype_pairs = [f'{k} {v}' for k,v in columns.items()]
    connection.execute(f"""
    DROP TABLE IF EXISTS {MAINTABLENAME};
    CREATE TABLE IF NOT EXISTS {MAINTABLENAME}({', '.join(column_datatype_pairs)});
    """)
    timer.stop()

    # Parse all the files in the datapath
    input_data_path = Path(datapath)
    if input_data_path.is_file():
        file_path_objects = [input_data_path]
    elif input_data_path.is_dir():
        file_path_objects = list(input_data_path.glob(file_pattern))
    else:
        raise FileExistsError()

    # Pre-process the input datasets
    timer = Timer('Pre-processing data')
    for i, fp in enumerate(file_path_objects):
        size = fp.stat().st_size
        print(f'\nProcessing file {i+1} of {len(file_path_objects)} (~{round(size/1e+9, 4)} GB)')
        with Progress(
            TextColumn('[progress.description]{task.description}'),
            SpinnerColumn(),
            TimeElapsedColumn()
        ) as progress_bars:
            task_stream = progress_bars.add_task(f'[red]Streaming CSV...', start=False)
            task_explode = progress_bars.add_task(f'[yellow]Exploding links...', start=False)
            task_parse = progress_bars.add_task(f'[green]Parsing URLs...', start=False)
            task_import = progress_bars.add_task(f'[blue]Importing data...', start=False)
            # Write selected CSV columns to parquet file
            progress_bars.start_task(task_id=task_stream)
            parquet_file = csv_to_parquet(fp)
            progress_bars.stop_task(task_id=task_stream)
            # Explode links in parquet file
            progress_bars.start_task(task_id=task_explode)
            explode_links(parquet_file, connection)
            progress_bars.stop_task(task_id=task_explode)
            # Parse URLs in database
            progress_bars.start_task(task_id=task_parse)
            polars_df = parse_links(connection)
            progress_bars.stop_task(task_id=task_parse)
            # Import data
            progress_bars.start_task(task_id=task_import)
            insert_data(connection)
            progress_bars.stop_task(task_id=task_import)
    timer.stop()


def csv_to_parquet(infile):
    name = infile.stem.split('.')[0]
    outfile = os.path.join('output', f'{name}.parquet')
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = select_columns
    convert_options.null_values = ['0']
    parser_options = pyarrow.csv.ParseOptions()
    parser_options.newlines_in_values = True
    writer = None
    with pyarrow.csv.open_csv(str(infile), convert_options=convert_options, parse_options=parser_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pyarrow.parquet.ParquetWriter(outfile, next_chunk.schema)
            next_table = pyarrow.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()
    return outfile


def explode_links(infile, connection):
    connection.execute(f"""
    DROP TABLE IF EXISTS input;
    """)
    connection.execute(f"""
    CREATE SEQUENCE IF NOT EXISTS serial START 1;
    """)
    connection.execute(f"""
    CREATE TABLE input AS
    SELECT NEXTVAL('serial') as seq, {select_columns_without_links}, UNNEST(link_list) as link
    FROM (
        SELECT STRING_SPLIT(p.links, '|') as link_list, {select_columns_without_links}
        FROM read_parquet('{infile}') p
        WHERE LEN(links) > 1
    );
    """)


def parse_links(connection):
    unnested_df = duckdb.table(table_name='input', connection=connection).pl()
    seq_array = []
    normalized_url_array = []
    domain_name_array = []
    with open('ural_errors.txt', 'a') as of:
        writer = csv.writer(of)
        for row in unnested_df.select(['seq', 'link']).iter_rows():
            link = str(row[1])
            id = row[0]
            seq_array.append(id)
            norm_url = ural_normalize_url(link)
            try:
                domain = ural_get_domain_name(norm_url)
            except:
                writer.writerow([link, norm_url])
                domain = None
            else:
                if domain in YOUTUBE_DOMAINS:
                    domain = 'youtube.com'
            normalized_url_array.append(norm_url)
            domain_name_array.append(domain)
    addendum_df = polars.DataFrame({
        'seq': seq_array,
        'normalized_url': normalized_url_array,
        'domain_name': domain_name_array
    })
    join = unnested_df.join(addendum_df, on="seq", how="left")
    df = join.drop(columns='seq')
    return df


def insert_data(connection):
    connection.execute(f"""
    INSERT INTO {MAINTABLENAME}
    SELECT *
    FROM polars_df
    """)
