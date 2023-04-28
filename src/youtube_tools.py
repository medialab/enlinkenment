import ural.youtube
from minet.youtube import YouTubeAPIClient
from minet.youtube.client import forge_channels_url, forge_videos_url
from minet.youtube.scrapers import scrape_channel_id
from ural.youtube import YoutubeChannel, YoutubeVideo


def get_youtube_metadata(url:str, config:dict):
    """Call YouTube API and return normalized data."""
    id, type = get_youtube_id(url)
    if id:
        data = call_youtube(id, type, config)
        if type == 'video' and data and len(data.get('items')) > 0 and data['items'][0].get('snippet', {}).get('channelId'):
            id = data['items'][0]['snippet']['channelId']
            supplemental_data = call_youtube(id, 'channel', config)
            data = {"video": data, "channel": supplemental_data}
        elif type == 'channel':
            data = {"channel": data}
        if type == 'video':
            return YoutubeVideoNormalizer(data)
        if type == 'channel':
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
            id = scrape_channel_id('https://'+url)
    return id, type


def call_youtube(id:str, type:str, config:dict):
    """Call YouTube API and return JSON response."""
    url = None
    key_list = config['youtube']['key_list']
    client = YouTubeAPIClient(key_list)
    if type == 'video':
        url = forge_videos_url([id])
    elif type == 'channel':
        url = forge_channels_url([id])
    if url:
        try:
            response = client.request_json(url)
            return response
        except Exception:
            pass


def verify_data_format(data:dict, key=None):
    is_format_ok = False
    if not data:
        return False

    if key and data.get(key):
        data = data[key]
    elif key and not data.get(key):
        return False

    return data.get('items') \
        and isinstance(data['items'], list) \
            and len(data['items']) > 0:


class BaseClass:
    def __init__(self) -> None:
        pass

    def as_dict(self):
        return self.__dict__


class YoutubeVideoNormalizer(BaseClass):
    def __init__(self, formatted_response) -> None:
        video_data, channel_data = self.parse_youtube_video_json_response(formatted_response)
        self.video_id = video_data.get('id')
        self.video_tags = video_data.get('snippet', {}).get('tags')
        self.video_publishedAt = video_data.get('snippet', {}).get('publishedAt')
        self.video_duration = video_data.get('contentDetails', {}).get('duration')
        self.video_commentCount = video_data.get('statistics', {}).get('commentCount')
        self.video_likeCount = video_data.get('statistics', {}).get('likeCount')
        self.video_favoriteCount = video_data.get('statistics', {}).get('favoriteCount')
        self.video_viewCount = video_data.get('statistics', {}).get('viewCount')
        self.video_title = video_data.get('snippet', {}).get('title')
        self.video_description = video_data.get('snippet', {}).get('description')
        self.channel_id = video_data.get('snippet', {}).get('channelId')
        self.channel_title = video_data.get('snippet', {}).get('channelTitle')
        self.channel_publishedAt = channel_data.get('snippet',{}).get('publishedAt')
        self.channel_viewCount = channel_data.get('statistics',{}).get('viewCount')
        self.channel_subscriberCount = channel_data.get('statistics',{}).get('subscriberCount')
        self.channel_videoCount = channel_data.get('statistics',{}).get('videoCount')
        self.channel_description = channel_data.get('brandingSettings',{}).get('channel', {}).get('description')
        self.channel_keywords = channel_data.get('brandingSettings',{}).get('channel', {}).get('keywords')
        self.channel_country = channel_data.get('brandingSettings',{}).get('channel', {}).get('country')

    def parse_youtube_video_json_response(self, data):
        video_data = {}
        if data.get('video') and data['video'].get('items') and len(data['video'].get('items')) > 0 \
            video_data = data['video']['items'][0]

        channel_data = {}
        if data.get('channel') and data['channel'].get('items') and len(data['channel'].get('items')) > 0:
            channel_data = data['channel']['items'][0]

        return video_data, channel_data


class YoutubeChannelNormalizer(BaseClass):
    def __init__(self, formatted_response) -> None:
        data = self.parse_youtube_channel_json_response(formatted_response)
        self.channel_id = data.get('id')
        self.channel_country = data.get('brandSettings', {}).get('channel', {}).get('country')
        self.channel_description = data.get('snippet', {}).get('description')
        self.channel_keywords = data.get('brandSettings', {}).get('channel', {}).get('keywords')
        self.channel_title = data.get('brandSettings', {}).get('channel', {}).get('title')
        self.channel_publishedAt = data.get('snippet', {}).get('publishedAt')
        self.channel_subscriberCount = data.get('statistics', {}).get('subscriberCount')
        self.channel_videoCount = data.get('statistics', {}).get('videoCount')
        self.channel_viewCount = data.get('statistics', {}).get('viewCount')

    def parse_youtube_channel_json_response(self, data):
        if data.get('channel') and data['channel'].get('items') and len(data['channel'].get('items')) > 0:
            return data['channel']['items'][0]
        return {}
