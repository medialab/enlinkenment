# Enlinkenment

A modular workflow for parsing and enriching URL data.

---
## Table of Contents
- [Installation](#installation)
- [Performance](#performance)
- [Workflow](#workflow)
---
## Installation
1. Create a new virtual environment with Python 3.11.
1. Clone the repository from GitHub onto your local machine.
    ```shell
    git clone git@github.com:medialab/enlinkenment.git
    cd enlinkenment
    ```
2. Install Python dependencies.
    ```shell
    pip install -r requirements.txt
    ```
3. Run the process [`src/main.py`](src/main.py) on your data file or on a directory containing data files with a `.csv` or `.gz` extension.
    ```shell
    python src/main.py ./DATA/DIRECTORY/
    ```

---
## Performance


---
## Workflow

### Input Data

1. Stream the CSV and select the relevant columns. Save them to a parquet file.
    - 3.5 - 4 minutes (for a file of ~26 GB)

2. Using `duckdb`, parse the parquet file and explode concatenated links in a tweet's `links` column.
    - 20-30 seconds

3. Parse those exploded links with `ural` and write the result to a new parquet file.
    - 7 - 15 minutes (depends on how many URLs are in the month's data)

4. Write the parsed URLs to a parquet file.
    - ~20 seconds

5. Parse the `local_time` field in all the processed parquet files and get a set of all the months in the data.

6. Create tables in the database for each month.

7. Again, parse the processed URL parquet file. This time, input the data in the proper month's table.

### Aggregate Data

```mermaid
flowchart LR

subgraph aggregate month's data
January2022
February2022
March2022
April2022
May2022
June2022
July2022
August2022
September2022
October2022
November2022
December2022
end

subgraph sum aggregates iteration 1
January2022 --> January2022_February2022
February2022 --> January2022_February2022
March2022 --> March2022_April2022
April2022 --> March2022_April2022
May2022 --> May2022_June2022
June2022 --> May2022_June2022
July2022 --> July2022_August2022
August2022 --> July2022_August2022
September2022 --> September2022_October2022
October2022 --> September2022_October2022
November2022 --> November2022_December2022
December2022 --> November2022_December2022
end

subgraph sum aggregates iteration 2
January2022_February2022 --> February2022_April2022
March2022_April2022 --> February2022_April2022
May2022_June2022 --> June2022_August2022
July2022_August2022 --> June2022_August2022
September2022_October2022 --> October2022_December2022
November2022_December2022 --> October2022_December2022
end

subgraph sum aggregates iteration 3
February2022_April2022 --> April2022_August2022
June2022_August2022 --> April2022_August2022
October2022_December2022 --> repeat_October2022_December2022[October2022_December2022]
end

subgraph sum aggregates iteration 4
April2022_August2022 --> August2022_December2022
repeat_October2022_December2022 --> August2022_December2022
end

subgraph create output table
August2022_December2022 --> domains
end


```
