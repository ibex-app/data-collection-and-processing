from app.model.platform import Platform

from app.core.downloaders.twitter.download_twitter import TwitterDownloader
from app.core.downloaders.facebook.download_facebook import FacebookDownloader
from app.core.downloaders.tv_georgia.download_geotv import TVGeorgiaDownloader
from app.core.downloaders.youtube.download_youtube import YoutubeDownloader


downloader_classes = {
    Platform.twitter: TwitterDownloader,
    Platform.facebook: FacebookDownloader,
    Platform.youtube: TVGeorgiaDownloader,
    Platform.geotv: YoutubeDownloader,
}
