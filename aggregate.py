# Workflow that parses and enriches incoming data file(s), 
# aggregate the URLs in the files, and enriches those aggregates.

import csv
import datetime
import gzip
import os
import subprocess
import timeit
from pathlib import Path
import datetime
from collections import Counter

import casanova
import click
from minet.cli.url_parse import REPORT_HEADERS
from tqdm.auto import tqdm

DATADIR = os.path.join('.', 'output') # change name later
TEMPDIR = os.path.join(DATADIR, 'temp')
INITIAL_AGGREG_HEADERS = ['raw_url', 'url_count', 'domain_count']

class FileNaming:
    def __init__(self, files) -> None:
        self.combined_stem = self.combine_file_names(files=files)
        self.temp_file_a = os.path.join(TEMPDIR, f'temp1_{self.combined_stem}.csv')
        self.temp_file_b = os.path.join(TEMPDIR, f'temp2_{self.combined_stem}.csv')
        self.aggreg_url_file = os.path.join(DATADIR, f'aggreg_{self.combined_stem}.csv')
        self.aggreg_domain_file = os.path.join(DATADIR, f'domains_{self.combined_stem}.csv')
        if not os.path.isdir(DATADIR):
            os.mkdir(DATADIR)
        if not os.path.isdir(TEMPDIR):
            os.mkdir(TEMPDIR)

    def single_file(self, datafile):
        path = Path(datafile)
        self.stem = path.stem.split('.')[0]
        self.fp = datafile
        self.preprocess_file = os.path.join(DATADIR, f'prep_{self.stem}.csv')
        self.url_parse_file = os.path.join(DATADIR, f'url-parse_{self.stem}.csv')
        self.frequency_file = os.path.join(DATADIR, f'frequency_{self.stem}.csv')
        self.temp_file_a = os.path.join(TEMPDIR, f'temp1_{self.stem}.csv')
        self.temp_file_b = os.path.join(TEMPDIR, f'temp2_{self.stem}.csv')

    def combine_file_names(self, files):
        l = []
        for file in files:
            stem = Path(file).stem.split('.')[0]
            l.extend(stem.split('_'))
        return Counter(l).most_common()[0][0]

    def remove_temp_files(self):
        if os.path.isfile(self.temp_file_a):
            os.remove(self.temp_file_a)
        if os.path.isfile(self.temp_file_b):
            os.remove(self.temp_file_b)
    
    def remove_temp_dir(self):
        os.rmdir(TEMPDIR)


class Scripts:
    def __init__(self, column:str) -> None:
        self.col = column

    def remove_retweets(self, outfile, infile):
        negative_search_retweets = ['xsv', 'search', '-v', '-s', '"retweeted_id"', '"."']
        positive_search_links = ['xsv', 'search', '-s', '"links"', '"."']
        with gzip.open(infile, 'r') as f:
            try:
                f.read(1)
            except:
                script = negative_search_retweets+[infile, '|']+positive_search_links[:-1]+['-o', outfile, '"."']
            else:
                script = ['gzcat', infile, '|']+negative_search_retweets+['|']+positive_search_links[:-1]+['-o', outfile, '"."']
        return True, " ".join(script)

    def preprocess(self, outfile, infile):
        script = ['xsv', 'explode', '-o', outfile, self.col, '"|"']
        with gzip.open(infile, 'r') as f:
            try:
                f.read(1)
            except:
                shell = False
                script.append(infile)
            else:
                shell = True
                script = " ".join(['gzcat', infile, '|']+script)
        return shell, script

    def url_parse(self, outfile, infile):
        return ['minet', 'url-parse', '-o', outfile, self.col, infile]

    def frequency(self, outfile, infile):
        return ['xsv', 'frequency', '-o', outfile, '-s', 'normalized_url,domain_name', '-l', '0', '--no-nulls', infile]

    def facebook(self, outfile, infile):
        return ['minet', 'url-parse', '--facebook', '-o', outfile, 'normalized_url', infile]

    def twitter(self, outfile, infile):
        return ['minet', 'url-parse', '--twitter', '-o', outfile, 'normalized_url', infile]

    def youtube(self, outfile, infile):
        return ['minet', 'url-parse', '--youtube', '-o', outfile, 'normalized_url', infile]


def run_subprocess(shell:bool, script, message:str):
    if isinstance(script,list):
        script_name = ' '.join(script)
    else:
        script_name = script
    print("\nRunning the {} command: {}\nThe subprocess's shell parameter is set to: {}".format(message, script_name, shell))
    print('Began at {}'.format(datetime.datetime.now().time()))
    start = timeit.default_timer()
    subprocess.run(script, shell=shell, text=True)
    stop = timeit.default_timer()
    delta = stop - start
    print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))


class Counts:
    def __init__(self) -> None:
        self.aggregated_source_ids = {}
        self.file_url_frequency = {}
        self.aggregated_url_frequency = {}
        self.file_domain_frequency = {}
        self.aggregated_domain_frequency = {}
        self.aggregated_tweet_pub_times = {}
        self.url_aggregates = {}

    def reset_frequency_tally(self):
        self.file_url_frequency = {}
        self.file_domain_frequency = {}


@click.command
@click.argument('data', nargs=-1, required=True)
@click.option('--url-col', nargs=1, required=True)
@click.option('--id-col', nargs=1, required=False)
@click.option('--tweets/--no-tweets', type=bool, required=False)
@click.option('--no-retweets/--retweets', type=bool, required=False)
def main(data, url_col, id_col, tweets, no_retweets):
    data_is_tweets = tweets

    fn = FileNaming(files=data)

    counts = Counts()
    for datafile in data:
        fn.single_file(datafile=datafile)
        scripts = Scripts(column=url_col)
        counts.reset_frequency_tally()

        # ------------------------------------------------------
        # Step 1. Pre-process and parse data

        if no_retweets:
            shell, remove_retweets_script = scripts.remove_retweets(outfile=fn.temp_file_a, infile=fn.fp)
            run_subprocess(shell=shell, script=remove_retweets_script, message='removing retweets')
            preprocess_infile = fn.temp_file_a
        else:
            preprocess_infile = fn.fp

        # Explode concatenated URLs
        shell, preprocess_script = scripts.preprocess(outfile=fn.preprocess_file, infile=preprocess_infile)
        run_subprocess(shell=shell, script=preprocess_script, message='pre-processing')
        
        # Apply minet's parse-url
        url_parse_script = scripts.url_parse(outfile=fn.temp_file_a, infile=fn.preprocess_file)
        run_subprocess(shell=False, script=url_parse_script, message='minet url-parse')
        os.remove(fn.preprocess_file)

        # Calculate the frequency of links' normalized URLs and domain names
        frequency_script = scripts.frequency(outfile=fn.frequency_file, infile=fn.temp_file_a)
        run_subprocess(shell=False, script=frequency_script, message='frequency')

        # ------------------------------------------------------
        # Step 2. Enrich parsed data with URL and domain counts

        # Serialize data from the frequency file 
        with open(fn.frequency_file) as f:
            reader = casanova.reader(f)
            for row in reader:
                if row[0] == 'normalized_url':
                    normalized_url = row[1]
                    if normalized_url == 'bit.ly/3SU4yqf':
                        print(row)
                    counts.file_url_frequency[normalized_url]=row[2]
                    if not counts.aggregated_url_frequency.get(normalized_url):
                        counts.aggregated_url_frequency[normalized_url] = int(row[2])
                    else:
                        counts.aggregated_url_frequency[normalized_url] = counts.aggregated_url_frequency[normalized_url] + int(row[2])
                if row[0] == 'domain_name':
                    domain_name = row[1]
                    counts.file_domain_frequency[domain_name]=row[2]
                    if not counts.aggregated_domain_frequency.get(domain_name):
                        counts.aggregated_domain_frequency[domain_name] = int(row[2])
                    else:
                        counts.aggregated_domain_frequency[domain_name] = counts.aggregated_domain_frequency[domain_name] + int(row[2])

        # While enriching data file with frequency counts, prepare aggregate of URL data in a dictionary
        start = timeit.default_timer()
        total = casanova.reader.count(fn.temp_file_a)
        with open(fn.temp_file_a) as f, open(fn.url_parse_file, 'w') as of:
            enricher = casanova.enricher(f, of, add=['url_count', 'domain_count'])

            url_pos = enricher.headers[url_col]
            normalized_url_pos = enricher.headers['normalized_url']
            domain_pos = enricher.headers['domain_name']
            pos_headers_to_keep = [enricher.headers[name] for name in REPORT_HEADERS]
            if id_col:
                id_pos = enricher.headers[id_col]
            if data_is_tweets:
                localtime_pos = enricher.headers['local_time']

            for row in tqdm(enricher, desc='Enriching & Aggregating URLs', total=total):

                rows_normalized_url = row[normalized_url_pos]
                url_frequency_across_this_datafile = counts.file_url_frequency.get(rows_normalized_url)
                domain_frequency_across_this_datafile = counts.file_domain_frequency.get(row[domain_pos])
                if id_col:
                    ids_associated_to_url_in_this_datafile = [row[id_pos]]

                # Write enriched row to url-parse CSV
                enricher.writerow(row, [url_frequency_across_this_datafile, domain_frequency_across_this_datafile])

                if rows_normalized_url:
                    # Prepare a dictionary to store as the value for this row's normalized URL in url_aggregates{}
                    url_data = {REPORT_HEADERS[i]:row[header_n] for i,header_n in enumerate(pos_headers_to_keep)}
                    url_data['raw_url'] = row[url_pos]
                    url_data['url_count'] = counts.aggregated_url_frequency[rows_normalized_url]
                    url_data['domain_count'] = counts.aggregated_domain_frequency[domain_name]

                    # If an ID column was given, update the aggregated tally of all IDs in the dataset associated with this row's URL
                    if id_col:
                        if not counts.aggregated_source_ids.get(rows_normalized_url):
                            counts.aggregated_source_ids[rows_normalized_url] = ids_associated_to_url_in_this_datafile
                        else:
                            counts.aggregated_source_ids[rows_normalized_url].extend(ids_associated_to_url_in_this_datafile)
                        url_data['source_ids'] = counts.aggregated_source_ids[rows_normalized_url]
                    
                    # If parsing Twitter data, update the aggregated tally of all publication times of tweets containing this row's URL
                    if data_is_tweets:
                        if not counts.aggregated_tweet_pub_times.get(rows_normalized_url):
                            counts.aggregated_tweet_pub_times[rows_normalized_url] = [row[localtime_pos]]
                        else:
                            counts.aggregated_tweet_pub_times[rows_normalized_url].append(row[localtime_pos])
                        url_data['tweet_localtimes'] = counts.aggregated_tweet_pub_times[rows_normalized_url]

                    # Update the dictionary that stores data for this URL in url_aggregates{}
                    counts.url_aggregates[rows_normalized_url]=url_data

        stop = timeit.default_timer()
        delta = stop - start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

        # Clean up enrichment of url-parse CSV
        print("Cleaning up temporary files and compressing enriched data file.\n")
        os.remove(fn.frequency_file)
        fn.remove_temp_files()
        subprocess.run(['gzip', fn.url_parse_file], check=True)


    # ------------------------------------------------------
    # Step 3. Write the URL aggregation to new CSV file
    if data_is_tweets:
        INITIAL_AGGREG_HEADERS.extend(['tweet_ids', 'tweet_localtimes', 'n_tweets_with_link'])
    elif id_col:
        INITIAL_AGGREG_HEADERS.extend(['source_ids', 'n_sources_with_url'])
    start = timeit.default_timer()
    with open(fn.aggreg_url_file, 'w') as urlfile:
        url_writer = csv.writer(urlfile)
        url_writer.writerow(REPORT_HEADERS+INITIAL_AGGREG_HEADERS)
        for url_data in tqdm(counts.url_aggregates.values(), desc='Writing Aggregates', total=len(counts.url_aggregates.items())):
            if id_col:
                url_data['source_count'] = len(url_data['source_ids'])
                if url_data['url_count'] != url_data['source_count']:
                    print('An error in the aggregation took place. The quantity of IDs associated with a URL must be equal to the number of times that URL was counted in the dataset.')
                    from pprint import pprint
                    pprint(url_data)
                    raise AssertionError
                url_data['source_ids'] = '|'.join(url_data['source_ids'])
            if data_is_tweets:
                url_data['tweet_localtimes'] = '|'.join(url_data['tweet_localtimes'])
            url_writer.writerow(url_data.values())
    stop = timeit.default_timer()
    delta = stop - start
    print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))

    # ------------------------------------------------------
    # Step 4. Enrich aggregates with domain-specific metadata

    # Parse all Facebook links
    facebook_script = scripts.facebook(outfile=fn.temp_file_a, infile=fn.aggreg_url_file)
    run_subprocess(shell=False, script=facebook_script, message='facebook url parse')

    # Parse all Twitter links
    twitter_script = scripts.twitter(outfile=fn.temp_file_b, infile=fn.temp_file_a)
    run_subprocess(shell=False, script=twitter_script, message='twitter url parse')

    # Parse all YouTube links
    youtube_script = scripts.youtube(outfile=fn.aggreg_url_file, infile=fn.temp_file_b)
    run_subprocess(shell=False, script=youtube_script, message='youtube url parse')

    # Clean up
    fn.remove_temp_files()
    fn.remove_temp_dir()


if __name__ == "__main__":
    main()
