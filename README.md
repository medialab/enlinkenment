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
26G compressed (December 2022) + 25G compressed (November 2022)

|step|action|duration|
|--|--|--|
|preprocess data|26G compressed file|0:03:49|
||25G compressed file|0:03:43|
|||
|data import|create main table scheme|0:00:00|
||read pre-processed 26G file to database|0:01:01|
||insert imported data to main table|0:00:17|
||read pre-processed 25G file to database|0:00:54|
||insert imported data to main table|0:00:25|
|||
|parse urls|explode links and relate to tweet id|0:02:04|
||aggregate links|0:00:10|
||parse unique links with URAL|0:03:28|
||compile cleaned parsed results to pyarrow table|0:00:06|
||create database table from pyarrow table|0:00:05|
|||
|aggregate links|de-aggregate links and enrich exploded links table|0:01:27|
||aggregate enriched links|0:03:51|
|||
|aggregate domains|associate domains to tweets|0:00:23|
||aggregate links by domain|0:02:37|

finished in 0:24:29

---
## Workflow

### Pre-process data
Large CSV file(s) are opened and streamed in chunks using `pyarrow.csv.open_csv()`. Only a selection of the columns in the CSV are parsed:
```
['id', 'timestamp_utc', 'local_time', 'retweet_count', 'like_count', 'reply_count', 'user_id', 'user_followers', 'user_friends', 'retweeted_id', 'retweeted_user_id', 'quoted_id', 'quoted_user_id', 'links']
```
Chunks of the selected columns are iteratively added to a `pyarrow` table, using `pyarrow.Table.from_batches()`, which is then iteratively written to a parquet file in the created subdirectory `output/`.

### Data import
The database management system `duckdb` then parses the select columns in the created parquet file and inserts them into a temporary table (`input`). Finally, `duckdb` merges the temporary `input` table into a central table (`tweets`) for all imported tweet data, avoiding any duplicates.
```mermaid
flowchart TD

input[/"full data file\n[csv]"/] -->pyarrow(pyarrow.csv reader\npyarrow.parquet writer)-->parquet[/"selected columns\n[parquet]"/]
parquet-->dbimport("duckdb\nread_parquet()")-->db[(database)]
db-->inputtable[table: input]
db===maintable[table: tweets]
inputtable-->merge(INSERT INTO tweets\nSELECT DISTINCT input.*\nFROM input\nLEFT JOIN tweets\nON input.id = tweets.id\nWHERE tweets.id IS NULL)-->maintable
style merge text-align:left
```

### Parse URLs
Writing to the disk/database, create a table (`link_tweet_relation`) of every association between every tweet and individual link in the main table.
```sql
CREATE SEQUENCE seq0;
INSERT INTO link_tweet_relation
SELECT NEXTVAL('seq0'), UNNEST(link_list) as link, tweet_id
FROM (
    SELECT STRING_SPLIT(s.links, '|') as link_list, id
    FROM (
        SELECT links, id
        FROM tweets
        WHERE links IS NOT NULL
    ) AS s
) tbl(link_list, tweet_id)
WHERE LEN(link_list) > 0;
```

To prevent parsing the same URL more than once, group by all links in the association table `link_tweet_relation` and aggregate the unique ID signifying the link's relation to a tweet.
```python
# Pull the table into memory
link_tweet_pairs = duckdb.table(
        table_name='link_tweet_relation',
        connection=connection
    )
# Use duckdb's aggregate method on the table
link_aggregate = link_tweet_pairs.aggregate(
        'link, STRING_AGG(id)'
    ).fetchall()
```

For each link, get the normalized version and the domain name. Then, to an in-memory `pyarrow` table, create columns for (a) the original link, (b) the normalized version, (c) the link's domain name, and (d) the aggregated list of the IDs that link has in the association table `link_tweet_relation` and write the parsed URL data to the `pyarrow table`. Finally, write the `pyarrow` table to disk/the database in a table.

```python
duckdb.from_arrow(arrow_object=aggregated_links_table, connection=connection).create(table_name='url_parse_results')
```

```mermaid
flowchart TD

subgraph Database
subgraph table : tweets
id["id\n1234567890"]
links["links\nhttps://www.google.com|https://www.github.com"]
end
links-->link1
links-->link2

subgraph table: link_tweet_relation
subgraph row 1
associationid1["id\npkey1"]
link1["link\nhttps://www.google.com"]
tweetid1["tweet_id\n1234567890"]
end
subgraph row 2
associationid2["id\npkey2"]
link2["link\nhttps://www.github.com"]
tweetid2["tweet_id\n1234567890"]
end
end
subgraph updated : link_tweet_relation
col4["id\npkey1"]
col3["link\nhttps://www.google.com"]
tweetid3["tweet_id\n1234567890"]
col1["normalized_url\ngoogle.com"]
col2["domain_name\ngoogle.com"]
pyarrowtable-->col1
pyarrowtable-->col2
end
end

subgraph Parse URLs in Python
link1--aggregate-->aggregate["(https://www.google.com, [pkey1])"]
aggregate-->parser(URAL parsing)
parser-->domain_name["domain name\ngoogle.com"]
parser-->normalized_url["normalized URL\ngoogle.com"]
parser-->link["link\nhttps://www.google.com"]
parser-->associationid["list of association ids\n[pkey1]"]
pyarrowtable[pyarrow table]
deaggregate("de-aggregate links")
deaggregate-->pyarrowtable
domain_name-->deaggregate
normalized_url-->deaggregate
link-->deaggregate
associationid-->deaggregate
end

```
### Aggregate links
By the normalized version of the URL, aggregate all the links in the association table (`link_tweet_relation`) and insert them into a new table (`links`).
```sql
INSERT INTO links
SELECT  normalized_id as id,
        ANY_VALUE(normalized_url) as normalized_url,
        ANY_VALUE(domain_name) as domain_name,
        ANY_VALUE(domain_id) as domain_id,
        ARRAY_AGG(DISTINCT tweet_id) as tweet_ids,
        ARRAY_AGG(DISTINCT link) as distinct_links,
FROM link_tweet_relation
GROUP BY normalized_id
```

### Aggregate domains
Join the main table of imported tweet data (`tweets`) and the table of enriched links (`links`) and group by the link's domain. To have a sum of tweets and users per domain, aggregate the tweet ID, retweet ID, and user ID with a count. For analyses about the frequency of the domain's links on the platform, various aggregation methods are used.
```sql
INSERT INTO aggregated_domains
SELECT  c.domain_id,
        ANY_VALUE(c.domain_name),
        COUNT(DISTINCT c.distinct_links),
        COUNT(DISTINCT c.tweet_id)-COUNT(DISTINCT c.retweeted_id),
        COUNT(DISTINCT c.retweeted_id),
        COUNT(DISTINCT c.tweet_id),
        COUNT(DISTINCT c.user_id),
        min(local_time),
        max(local_time),
        datediff('day', min(local_time), max(local_time)),
        histogram(date_trunc('month', local_time)),
FROM (
    SELECT  b.tweet_id,
            a.user_id,
            a.retweeted_id,
            a.quoted_id,
            b.domain_name,
            b.domain_id,
            b.distinct_links,
            a.local_time
    FROM tweets a
    JOIN (
        SELECT UNNEST(tweet_ids) as tweet_id, domain_id, domain_name, distinct_links
        FROM links
    ) b
    ON b.tweet_id = a.id
) c
WHERE c.domain_id IS NOT NULL
GROUP BY c.domain_id
```
In a new `GROUP BY` that groups the join by both a link's domain its month of publication, count the number of tweets per month.
```sql
UPDATE aggregated_domains
    SET nb_tweets_per_month_with_domain = s.count_per_month
    FROM (
        SELECT  COUNT(tweet_id) as count_per_month, domain_id
        FROM (
            SELECT  b.tweet_id,
                    a.local_time,
                    b.domain_id,
                    b.domain_name
            FROM tweets a
            JOIN (
                SELECT UNNEST(tweet_ids) as tweet_id, domain_id, domain_name
                FROM links
            ) b
            ON b.tweet_id = a.id
        ) c
        WHERE c.domain_id IS NOT NULL
        GROUP BY DATEPART('month', local_time), domain_id
    ) s
    WHERE aggregated_domains.id = s.domain_id;
```
