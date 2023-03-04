import ast
import os
import re
import click
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

from CONSTANTS import AGGREGATEDDOMAINSTABLE, LINKSTABLENAME, MAINTABLENAME
from utils import Timer

@click.command()
@click.option('-o', '--output-dir', required=False)
def main(output_dir):


    if not output_dir or Path(output_dir).is_file():
        output_dir = 'output'
    database = os.path.join(output_dir, 'twitter_links.db')
    connection = duckdb.connect(database=database, read_only=False)
    connection.execute('PRAGMA enable_progress_bar')

    timer = Timer('Writing histograms for high-volume domains')
    data = duckdb.query(f"""
    SELECT histogram_of_tweets_per_month, domain, nb_tweets_per_month_with_domain
    FROM {AGGREGATEDDOMAINSTABLE}
    """, connection=connection).fetchall()
    max_per_month = duckdb.table(
        table_name=AGGREGATEDDOMAINSTABLE,
        connection=connection).max('nb_tweets_per_month_with_domain').fetchone()[0]
    histogram_dir = os.path.join('output', 'histograms')
    os.makedirs(histogram_dir, exist_ok=True)
    for tuple in data:
        domain = tuple[1]
        nb_tweets_per_month_with_domain = tuple[2]
        histogram = str(tuple[0]).replace('=',':')
        histogram = re.sub(
                        pattern=r'(\d{4}-\d{2}-\d{2})', 
                        repl='"\\1"', 
                        string=histogram
                    )
        dictionary = ast.literal_eval(histogram)
        if len(list(dictionary.keys())) > 6 and nb_tweets_per_month_with_domain > 100:
            plt.title(f"Tweets per month for {domain}")
            # x = [datetime.strptime(k, '%Y-%m-%d') for k in list(dictionary.keys())]
            x = list(dictionary.keys())
            y = dictionary.values()
            plt.bar(x, y, color='g')
            plt.ylim(-2, max_per_month+10)
            outfile_path = os.path.join(histogram_dir, f'{domain}.png')
            plt.savefig(outfile_path)
    timer.stop()
