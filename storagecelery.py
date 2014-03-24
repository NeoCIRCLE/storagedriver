from celery import Celery
from kombu import Queue, Exchange
from socket import gethostname
from os import getenv
HOSTNAME = gethostname()
AMQP_URI = getenv('AMQP_URI')
CACHE_URI = getenv('CACHE_URI')

celery = Celery('storagedriver',
                broker=AMQP_URI,
                include=['storagedriver'])

celery.conf.update(
    CELERY_RESULT_BACKEND='cache',
    CELERY_CACHE_BACKEND=CACHE_URI,
    CELERY_TASK_RESULT_EXPIRES=300,
    CELERY_QUEUES=(
        Queue(HOSTNAME + '.storage', Exchange(
            'storagedriver', type='direct'), routing_key='storagedriver'),
    )
)
