from celery import Celery

from app.config.constants import CeleryConstants as CC


celery = Celery("ibex tasks",
                broker=CC.LOCAL_BROKER_URL_REDIS,
                backend=CC.LOCAL_RESULT_BACKEND_REDIS,
                include=[
                    'app.core.celery.tasks.collect', 
                    'app.core.celery.tasks.download', 
                    'app.core.celery.tasks.process']
                )

celery.conf.update(
    result_expires=CC.EXPIRE,
)
