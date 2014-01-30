from disk import Disk
from storagecelery import celery
from os import path, unlink

@celery.task()
def list(dir):
    return [d.get_desc() for d in Disk.list(dir)]


@celery.task()
def create(disk_desc):
    disk = Disk.deserialize(disk_desc)
    disk.create()


@celery.task()
def delete(json_data):
    disk = Disk.deserialize(json_data)
    disk.delete()


@celery.task()
def delete_dump():
    if path.endswith(".dump") and os.path.isfile(path):
        os.unlink(path)


@celery.task()
def snapshot(json_data):
    disk = Disk.deserialize(json_data)
    disk.snapshot()


@celery.task()
def merge(old_json, new_json):
    disk = Disk.deserialize(old_json)
    new_disk = Disk.deserialize(new_json)
    disk.merge(new_disk)


@celery.task()
def get(json_data):
    disk = Disk.get(dir=json_data['dir'], name=json_data['name'])
    return disk.get_desc()
