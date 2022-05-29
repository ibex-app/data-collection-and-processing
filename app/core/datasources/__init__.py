from ibex_models import Platform

from app.core.datasources.twitter.search_twitter import TwitterCollector
from app.core.datasources.facebook.search_facebook import FacebookCollector
from app.core.datasources.geo_tv.search_geotv import TVGeorgiaCollector
from app.core.datasources.youtube.search_youtube import YoutubeCollector
from app.core.datasources.telegram.search_telegram import TelegramCollector
from app.core.datasources.vkontakte.search_vkontakte import VKCollector


collector_classes = {
    Platform.twitter: TwitterCollector,
    Platform.facebook: FacebookCollector,
    Platform.youtube: YoutubeCollector,
    # Platform.geotv: TVGeorgiaCollector,
    Platform.telegram: TelegramCollector,
    Platform.vkontakte: VKCollector
}
