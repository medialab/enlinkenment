import casanova
from pathlib import Path
import itertools
import subprocess


def call_youtube_videos(infile: Path, outfile: Path, keys: list):
    """Function to get channel ID of all YouTube links.

    Args:
        infile (Path): path to CSV file of video links
        outfile (Path): path to CSV file in which to write video metadata
        keys (list): YouTube API keys
    """
    total = casanova.reader.count(infile)
    key_args = transform_key_list_into_subprocess_args(keys)
    minet_yt_channel_command = (
        [
            "minet",
            "yt",
            "videos",
            "-o",
            str(outfile),
            "--total",
            str(total),
        ]
        + key_args
        + [
            "normalized_url",
            "-i",
            str(infile),
        ]
    )
    subprocess.run(minet_yt_channel_command, check=True)


def transform_key_list_into_subprocess_args(keys: list) -> list[str]:
    """Function to convert array of keys into array useable in subprocess.

    Args:
        keys (list): YouTube API keys

    Returns:
        list[str]: array alternating "-k" and key
    """
    key_options = list(
        itertools.chain.from_iterable(list(zip(itertools.repeat("-k"), keys)))
    )
    return key_options
