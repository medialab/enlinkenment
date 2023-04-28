import json

import casanova
import click
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn)

from youtube_tools import YoutubeChannelNormalizer, get_youtube_metadata


@click.command()
@click.option('-i', '--infile')
@click.option('-o', '--outfile')
@click.option('-c', '--config-file')
@click.option('-k', '--key', multiple=True, required=False)
def main(infile, outfile, config_file, key):

    if config_file:
        with open(config_file, 'r') as f:
            config = json.load(fp=f)
    elif key:
        key_list = list(key)
        config = {'youtube':{'key_list':key_list}}


    total = casanova.reader.count(infile)
    with open(infile) as f, open(outfile, 'w') as of:
        enricher = casanova.enricher(f, of, add=['channel_country', 'channel_description', 'channel_id', 'channel_keywords', 'channel_publishedAt', 'channel_subscriberCount', 'channel_title', 'channel_videoCount', 'channel_viewCount', 'video_commentCount', 'video_description', 'video_duration', 'video_favoriteCount', 'video_id', 'video_likeCount', 'video_publishedAt', 'video_tags', 'video_title', 'video_viewCount'])
        ProgressCompleteColumn = Progress(
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            BarColumn(bar_width=60),
            TimeElapsedColumn(),
            expand=True,
            )
        with ProgressCompleteColumn as progress:
            task1 = progress.add_task(description=f'[bold green]Requesting YouTube data channel...', total=total, start=True)
            for row, url in enricher.cells('normalized_url', with_rows=True):
                normalized_data = get_youtube_metadata(url, config)
                if normalized_data:
                    supplement = format_youtube_data(normalized_data)
                    enricher.writerow(row, supplement)
                progress.update(task_id=task1, advance=1)


def format_youtube_data(normalized_data):
    additional_attributes = []
    if isinstance(normalized_data, YoutubeChannelNormalizer):
        additional_attributes = ['video_id', 'video_description', 'video_tags', 'video_publishedAt', 'video_duration', 'video_favoriteCount', 'video_title', 'video_viewCount', 'video_likeCount', 'video_commentCount']
    data_as_dict = normalized_data.as_dict()
    for attribute in additional_attributes:
        data_as_dict.update({attribute:None})
    sorted_data = sorted(data_as_dict.items())
    return [value for _,value in sorted_data]


if __name__ == "__main__":
    main()
