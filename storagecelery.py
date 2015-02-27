from celery import Celery
from kombu import Queue, Exchange
from os import getenv
from argparse import ArgumentParser


parser = ArgumentParser()
parser.add_argument("-n", "--hostname", dest="hostname",
                    help="Define the full queue name with"
                    "with priority", metavar="hostname.queue.priority")
(args, unknwon_args) = parser.parse_known_args()
HOSTNAME = vars(args).pop("hostname")
if HOSTNAME is None:
    raise Exception("You must define hostname as -n <hostname> or "
                    "--hostname=<hostname>.\n"
                    "Hostname format must be hostname.module.priority.")

AMQP_URI = getenv('AMQP_URI')

celery = Celery('storagedriver',
                broker=AMQP_URI,
                include=['storagedriver'])

celery.conf.update(
    CELERY_RESULT_BACKEND='amqp',
    CELERY_TASK_RESULT_EXPIRES=300,
    CELERY_QUEUES=(
        Queue(HOSTNAME, Exchange(
            'storagedriver', type='direct'), routing_key='storagedriver'),
    )
)
