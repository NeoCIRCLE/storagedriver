from disk import Disk
from storagecelery import celery


@celery.task()
def list_disks():
    return Disk.list('/home/cloud/images')


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
