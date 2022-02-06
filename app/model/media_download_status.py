from enum import Enum

class MediaDownloadStatus(str, Enum):
    to_be_downloaded = 'to_be_downloaded'
    downloaded = 'downloaded'
    no_download = 'no_download'