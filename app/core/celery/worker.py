from celery import Celery

from app.config.constants import CeleryConstants as CC


celery = Celery("ibex tasks",
                broker=CC.CLOUD_BROKER_URL,
                backend=CC.LOCAL_RESULT_BACKEND_SQLLITE,
                include=[
                    'app.core.celery.tasks.collect', 
                    'app.core.celery.tasks.download', 
                    'app.core.celery.tasks.process']
                )

celery.conf.update(
    result_expires=CC.EXPIRE,
)
