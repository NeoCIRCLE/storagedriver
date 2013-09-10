from disk import Disk
from storagecelery import celery


@celery.task()
def list_disks(dir):
    return [d.get_desc() for d in Disk.list(dir)]


@celery.task()
def create_disk(disk_desc):
    disk = Disk.deserialize(disk_desc)
    disk.create()


@celery.task()
def delete_disk(json_data):
    disk = Disk.import_from_json(json_data)
    disk.delete()


@celery.task()
def get_disk(json_data):
    disk = Disk.import_from_json(json_data)
    return jsonpickle.encode(disk, unpicklable=False)
