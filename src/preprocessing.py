from pathlib import Path

import duckdb
import polars
import pyarrow
import pyarrow.csv
import pyarrow.parquet
import ural
import ural.youtube
from rich.progress import (
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from utilities import FileNaming, get_filepaths, style_panel

# Columns to be selected from raw Twitter file
SELECT_COLUMNS = ["id", "local_time", "user_id", "retweeted_id", "links"]

# Columns from raw Twitter file to keep after parsing
UNALTERED_COLUMNS = ["id", "local_time", "user_id", "retweeted_id"]

# Columns to add after parsing
FINAL_PREPROCESSING_COLUMNS = UNALTERED_COLUMNS + ["link", "domain"]

# Prefix for pre-resolution CSV files
PARSED_URL_PREFIX = "parsed_urls"
PARSED_URL_FILE_PATTERN = PARSED_URL_PREFIX + "*.parquet"


def parse_input(
    input_data_path: Path, input_file_pattern: str, output_dir: Path, color: str
):
    """
    Iterating over each file captured by the input file pattern, this function manages the 3 steps of pre-processing:

        (1) Stream the CSV file and select the relevant columns.

        (2) De-concatenate and unnest the URLs in the "links" column.

        (3) Parse the isolated URLs with Ural, generating new columns for the domain name and the normalized version of each URL.
    """

    msg = f"""
Iterating over each targeted data file:
  (1) Stream the CSV file and select the relevant columns.
  (2) De-concatenate and unnest the URLs in the "links" column.
  (3) Parse the isolated URLs with Ural, generating new columns for the domain name and the normalized version of each URL.

The resulting parsed data are written to compressed parquet files in the directory "{str(output_dir)}" with the prefix "{PARSED_URL_PREFIX}".
    """
    style_panel(msg=msg, color=color, title="Pre-process data")

    # Using the file path pattern, get an array of files to process
    files = get_filepaths(input_data_path, input_file_pattern)

    # ----------------------------------------------------------------------- #
    # Set up the progress bar
    with Progress(
        TextColumn("{task.description}"),
        SpinnerColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ) as progress:
        total = total = len(files)
        file_task = progress.add_task(
            description=f"{color}Processing files...", total=total, start=True
        )
        for n, infile in enumerate(files):
            name_file = FileNaming(output_dir, infile)

            step1 = progress.add_task(
                description=f"[yellow]    Step 1. select columns...",
                total=total,
                start=False,
            )
            progress.update(task_id=step1, completed=n)
            step2 = progress.add_task(
                description=f"[yellow]    Step 2. de-concatenate links...",
                total=total,
                start=False,
            )
            progress.update(task_id=step2, completed=n)
            step3 = progress.add_task(
                description=f"[yellow]    Step 3. parse links...",
                total=total,
                start=False,
            )
            progress.update(task_id=step3, completed=n)
            # -------------------------------------------------------------- #

            # Select relevant columns from CSV file
            task = step1
            progress.start_task(task_id=task)
            selected_columns_outfile = name_file.parquet("selected_columns")
            select_columns(infile, selected_columns_outfile)
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n + 1)

            # De-concatenate URLs in "links" column
            task = step2
            progress.start_task(task_id=task)
            deconcatenate_links_dataframe = deconcatenate_links(
                selected_columns_outfile
            )
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n + 1)

            # Parse links
            task = step3
            progress.start_task(task_id=task)
            parsed_urls_outfile = name_file.parquet(PARSED_URL_PREFIX)
            parse_links(deconcatenate_links_dataframe, parsed_urls_outfile)
            progress.stop_task(task_id=task)
            progress.update(task_id=task, completed=n + 1)

            # Before moving to next file, update progress bar
            progress.update(task_id=file_task, advance=1)
            progress.remove_task(task_id=step1)
            progress.remove_task(task_id=step2)
            progress.remove_task(task_id=step3)


def configure_pyarrow(columns):
    """Function to configure how pyarrow streams a CSV."""
    convert_options = pyarrow.csv.ConvertOptions()
    convert_options.include_columns = columns
    convert_options.null_values = ["0"]
    parser_options = pyarrow.csv.ParseOptions()
    parser_options.newlines_in_values = True
    return convert_options, parser_options


def select_columns(infile: Path, outfile: Path, columns: list = SELECT_COLUMNS):
    """Step 1 in pre-processing. This function streams a CSV file and writes certain columns to a parquet file."""
    infile_path_name = str(infile)
    convert_options, parser_options = configure_pyarrow(columns)
    writer = None
    with pyarrow.csv.open_csv(
        infile_path_name, convert_options=convert_options, parse_options=parser_options
    ) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pyarrow.parquet.ParquetWriter(outfile, next_chunk.schema)
            next_table = pyarrow.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    if writer:
        writer.close()


def deconcatenate_links(infile: Path) -> polars.DataFrame:
    """Step 2 in pre-processing. This function reads a parquet file and de-concatenates the URLs in column "links," which are separated by a |."""
    unaltered_columns = ", ".join(UNALTERED_COLUMNS)
    query = f"""
    SELECT {unaltered_columns}, UNNEST(links_list) AS link
    FROM (
        SELECT *, STRING_SPLIT(p.links, '|') as links_list
        FROM read_parquet('{str(infile)}') p
        WHERE LEN(links) > 1
    );
    """
    return duckdb.from_query(query).pl()


def attribute_domain(normalized_url: str):
    """Function to adjust Ural's get_domain_name method so that every parsed version of a YouTube domain name is written the same."""
    try:
        domain = ural.get_domain_name(normalized_url)
    except Exception:
        domain = None
    else:
        if domain in ural.youtube.YOUTUBE_DOMAINS:
            domain = "youtube.com"
    return domain


def parse_links(in_dataframe: polars.DataFrame, outfile: Path):
    """Step 3 in pre-processing. This function parses the dataframe's URL data and adds columns with a normalized URL and domain name."""
    df_with_normalized_url = in_dataframe.with_columns(
        [
            polars.col("link").apply(ural.normalize_url).alias("normalized_url"),
        ]
    )
    df_with_normalized_url.with_columns(
        [
            polars.col("normalized_url").apply(attribute_domain).alias("domain"),
        ]
    ).write_parquet(file=outfile, compression="gzip")
