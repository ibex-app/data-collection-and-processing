from celery import Celery

from app.config.constants import CeleryConstants as CC


celery = Celery("ibex tasks",
                broker=CC.BROKER_URL,
                backend=CC.RESULT_BACKEND,
                include=[
                    'app.core.celery.tasks.collect', 
                    'app.core.celery.tasks.download', 
                    'app.core.celery.tasks.process']
                )

celery.conf.update(
    result_expires=CC.EXPIRE,
)
