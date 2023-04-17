import ural.youtube
from minet.youtube import YouTubeAPIClient
from minet.youtube.client import forge_channels_url, forge_videos_url
from minet.youtube.scrapers import scrape_channel_id
from ural.youtube import YoutubeChannel, YoutubeVideo


def get_youtube_metadata(url:str, config:dict):
    """Call YouTube API and return normalized data."""
    id, type = get_youtube_id(url)
    if type == 'channel':
        data = call_youtube(id, type, config)
        # Store data in formatted dictionary
        if data and type == 'channel':
            data = {'channel': data}
            # Normalize the data
            return YoutubeChannelNormalizer(data)


def get_youtube_id(url:str):
    """Return YouTube URL's type and ID."""
    id, type = None, None
    parsed_url = ural.youtube.parse_youtube_url(url)
    if isinstance(parsed_url, YoutubeVideo):
        type = 'video'
        id = parsed_url.id
    elif isinstance(parsed_url, YoutubeChannel):
        type = 'channel'
        id = parsed_url.id
        if not id:
            id = scrape_channel_id(url)
    return id, type


def call_youtube(id:str, type:str, config:dict):
    """Call YouTube API and return JSON response."""
    url = None
    key = config['youtube']['key']
    client = YouTubeAPIClient(key)
    if type == 'video':
        url = forge_videos_url([id])
    elif type == 'channel':
        url = forge_channels_url([id])
    if url:
        try:
            response = client.request_json(url)
            return response
        except Exception as e:
            pass


class BaseClass:
    def __init__(self) -> None:
        pass

    def as_dict(self):
        return self.__dict__

    def as_row(self):
        dict = self.as_dict()
        return list(dict.values())


class YoutubeChannelNormalizer(BaseClass):
    def __init__(self, formatted_response) -> None:
        data = self.parse_youtube_channel_json_response(formatted_response)
        self.identifier = data.get('id')
        self.countryOfOrigin = data.get('brandSettings', {}).get('channel', {}).get('country')
        self.description = data.get('snippet', {}).get('description')
        self.keywords = data.get('brandSettings', {}).get('channel', {}).get('keywords')
        self.name = data.get('brandSettings', {}).get('channel', {}).get('title')
        self.dateCreated = data.get('snippet', {}).get('publishedAt')
        self.subscriberCount = data.get('statistics', {}).get('subscriberCount')
        self.videoCount = data.get('statistics', {}).get('videoCount')
        self.viewCount = data.get('statistics', {}).get('viewCount')

    def parse_youtube_channel_json_response(self, data):
        if data.get('channel') and verify_items_list(data['channel']):
            return data['channel']['items'][0]
        else:
            return {}


def verify_items_list(data:dict):
    if data.get('items') and isinstance(data['items'],list) and len(data['items']) > 0:
        return True
    else:
        return False
