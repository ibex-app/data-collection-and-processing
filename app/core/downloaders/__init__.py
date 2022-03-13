from ibex_models import Platform

from app.core.downloaders.twitter.download_twitter import TwitterDownloader
from app.core.downloaders.facebook.download_facebook import FacebookDownloader
from app.core.downloaders.geo_tv.download_geotv import TVGeorgiaDownloader
from app.core.downloaders.youtube.download_youtube import YoutubeDownloader


downloader_classes = {
    Platform.twitter: TwitterDownloader,
    Platform.facebook: FacebookDownloader,
    Platform.youtube: YoutubeDownloader,
    Platform.geotv: TVGeorgiaDownloader,
}
