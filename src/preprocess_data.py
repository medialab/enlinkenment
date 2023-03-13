import csv
import os

import duckdb
import polars
import pyarrow
import pyarrow.csv
import pyarrow.parquet

import datetime

from ural import get_domain_name as ural_get_domain_name
from ural import normalize_url as ural_normalize_url
from ural.youtube import YOUTUBE_DOMAINS

from constants import SELECT_COLUMNS, MAIN_TABLE_DATA_TYPES
from utils import Timer, get_filepaths

from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                           SpinnerColumn, TextColumn, TimeElapsedColumn)


# 
def process_data(data, file_pattern, output_dir):
    """Main function parse Tweet data."""

    file_path_objects = get_filepaths(data, file_pattern)

    # Pre-process the input datasets
    timer = Timer('Pre-processing data')
    for i, fp in enumerate(file_path_objects):
        size = fp.stat().st_size
        print(f'\nProcessing file {i+1} of {len(file_path_objects)} (~{round(size/1e+9, 4)} GB) : {fp}')
        ProgressSpinnerColumn = Progress(
                        TextColumn('[progress.description]{task.description}'),
                        SpinnerColumn(),
                        TimeElapsedColumn()
                        )
        with ProgressSpinnerColumn as progress_bars:
            tasks = PreprocessingTasks(fp, output_dir, progress_bars)
            tasks.streaming_csv(csv_to_parquet)
            tasks.exploding_links()
            tasks.parsing_links(parse_links)
            tasks.writing_data()

    timer.stop()


class PreprocessingTasks():
    """Class to manage progress bars' tasks variables."""
    def __init__(self, infile, output_dir, progress_bars) -> None:
        self.infile = infile
        self.output_dir = output_dir
        self.progress_bars = progress_bars
        self.task_id1 = self.add_task(color='red', task='Streaming CSV')
        self.task_id2 = self.add_task(color='yellow', task='Exploding links')
        self.task_id3 = self.add_task(color='green', task='Parsing domains')
        self.task_id4 = self.add_task(color='blue', task='Writing processed data')

    def add_task(self, color, task):
        message = f'[{color}]'+task+'...'
        return self.progress_bars.add_task(message, start=False)

    def streaming_csv(self, fn):
        self.progress_bars.start_task(self.task_id1)
        self.parquet_outfile = fn(self.infile, self.output_dir)
        self.progress_bars.stop_task(self.task_id1)

    def exploding_links(self):
        self.progress_bars.start_task(self.task_id2)
        columns = SELECT_COLUMNS[:-1] 
        self.polars_df = duckdb.from_query(f"""
        SELECT {', '.join(columns)}, UNNEST(link_list) as link
        FROM (
            SELECT *, STRING_SPLIT(p.links, '|') as link_list
            FROM read_parquet('{self.parquet_outfile}') p
            WHERE LEN(links) > 1
        );
        """).pl()
        self.progress_bars.stop_task(self.task_id2)

    def parsing_links(self, fn):
        self.progress_bars.start_task(self.task_id3)
        self.polars_df = fn(self.polars_df)
        self.progress_bars.stop_task(self.task_id3)

    def writing_data(self):
        self.progress_bars.start_task(self.task_id4)
        os.remove(self.parquet_outfile)
        self.polars_df.write_parquet(
            file=self.parquet_outfile,
            compression='zstd'
        )
        self.progress_bars.stop_task(self.task_id4)
        print(duckdb.from_query(f"""
        SELECT *
        FROM read_parquet('{self.parquet_outfile}');
        """).describe())


def csv_to_parquet(infile, output_dir):
    """Function to stream CSV and write select columns to parquet."""
    name = infile.stem.split('.')[0]
    outfile = os.path.join(output_dir, f'{name}.parquet')
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = SELECT_COLUMNS
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


def parse_links(unnested_df):
    """Parse links in Polars data frame."""
    normalized_url_array = []
    domain_name_array = []
    with open('ural_errors.txt', 'a') as of:
        writer = csv.writer(of)
        for row in unnested_df.iter_rows():
            link = str(row[-1])
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
    existing = {
        column:unnested_df.get_column(column)
        for column in unnested_df.columns
    }
    addendum = {
        'normalized_url': normalized_url_array,
        'domain_name': domain_name_array
    }
    return polars.DataFrame(existing | addendum)


def insert_processed_data(connection, output_dir):
    """Function to insert parquet file into database's main table."""
    connection.execute('PRAGMA disable_progress_bar')

    # Get list of processed parquet files
    parquet_files = get_filepaths(
        data=output_dir,
        file_pattern='**/*.parquet'
    )
    ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
    with ProgressCompleteColumn as progress:
        task1 = progress.add_task('[red]Parsing date range...', total=len(parquet_files))

        # Set for distinct months in all files
        all_months = []
        # Pair each filepath with a list of (month, table name)
        months_per_file = {}

        # Get a list of months in each file
        for parquet_filepath in parquet_files:
            distinct_months = [t[0] for t in 
                             duckdb.sql(f"""
                                        SELECT DISTINCT date_trunc('month', local_time)
                                        FROM read_parquet('{parquet_filepath}')
                                        GROUP BY date_trunc('month', local_time);
                             """).fetchall()]
            months_per_file[parquet_filepath]=distinct_months
            all_months.extend(distinct_months)
            progress.update(task_id=task1, advance=1)

        task2 = progress.add_task('[green]Create table...', total=len(all_months))
        task3 = progress.add_task('[blue]Insert data...', total=len(months_per_file))
        columns = ', '.join([k+' '+v for k,v in MAIN_TABLE_DATA_TYPES.items()])
        for month in list(set(all_months)):
            table_name = datetime.datetime.strftime(month, "%B")+\
                    datetime.datetime.strftime(month, "%Y")
            connection.execute(f"""
            CREATE TABLE {table_name}({columns});
            """)
            progress.update(task_id=task2, advance=1)

        for parquet_filepath, months in months_per_file.items():
            for month in months:
                table_name = table_name = datetime.datetime.strftime(month, "%B")+\
                    datetime.datetime.strftime(month, "%Y")
                connection.execute(f"""
                INSERT INTO {table_name}
                SELECT *
                FROM read_parquet('{parquet_filepath}')
                WHERE date_trunc('month', local_time) = '{month}';
                """)
                print(f'created table: {table_name}')
                print(duckdb.table(table_name, connection).describe())
                progress.update(task_id=task3, advance=1)
