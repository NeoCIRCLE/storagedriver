import jsonpickle

from celery import Celery

from disk import Disk

BROKER_URL = 'amqp://nyuszi:teszt@localhost:5672/django'
celery = Celery('tasks', broker=BROKER_URL, backend='amqp')
celery.config_from_object('celeryconfig')


@celery.task()
def list_disks():
    return jsonpickle.encode(Disk.list('/home/cloud/images'),
                             unpicklable=False)


@celery.task()
def create_disk(json_data):
    disk = Disk.import_from_json(json_data)
    disk.create()


@celery.task()
def delete_disk(json_data):
    disk = Disk.import_from_json(json_data)
    disk.delete()


@celery.task()
def get_disk(json_data):
    disk = Disk.import_from_json(json_data)
    return jsonpickle.encode(disk, unpicklable=False)
