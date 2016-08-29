from disk import Disk
from storagecelery import celery
from os import path, unlink, statvfs, listdir, mkdir
from shutil import move
from celery.contrib.abortable import AbortableTask
import logging

logger = logging.getLogger(__name__)

trash_directory = "trash"


@celery.task()
def list(dir):
    return [d.get_desc() for d in Disk.list(dir)]


@celery.task()
def list_files(datastore):
    return [l for l in listdir(datastore) if
            path.isfile(path.join(datastore, l))]


@celery.task()
def create(disk_desc):
    disk = Disk.deserialize(disk_desc)
    disk.create()


class download(AbortableTask):
    time_limit = 18000  # TODO: calculate proper value it's 5h now

    def run(self, **kwargs):
        disk_desc = kwargs['disk']
        url = kwargs['url']
        parent_id = kwargs.get("parent_id", None)
        disk = Disk.deserialize(disk_desc)
        disk.download(self, url, parent_id)
        return {'size': disk.size,
                'type': disk.format,
                'checksum': disk.checksum, }


@celery.task()
def delete(json_data):
    disk = Disk.deserialize(json_data)
    disk.delete()


@celery.task()
def delete_dump(disk_path):
    if disk_path.endswith(".dump") and path.isfile(disk_path):
        unlink(disk_path)


@celery.task()
def snapshot_from_base(json_data):
    disk = Disk.deserialize(json_data)
    disk.snapshot_from_base()


@celery.task()
def snapshot(disk_desc, snapshot_name):
    disk = Disk.deserialize(disk_desc)
    disk.snapshot(snapshot_name)


@celery.task()
def list_snapshots(disk_desc):
    disk = Disk.deserialize(disk_desc)
    return disk.list_snapshots()


@celery.task()
def remove_snapshot(disk_desc, snapshot_id):
    disk = Disk.deserialize(disk_desc)
    disk.remove_snapshot(snapshot_id)


@celery.task()
def revert_snapshot(disk_desc, snapshot_id):
    disk = Disk.deserialize(disk_desc)
    disk.revert_snapshot(snapshot_id)


class merge(AbortableTask):
    time_limit = 18000

    def run(self, **kwargs):
        old_json = kwargs['old_json']
        new_json = kwargs['new_json']
        parent_id = kwargs.get("parent_id", None)
        disk = Disk.deserialize(old_json)
        new_disk = Disk.deserialize(new_json)
        disk.merge(self, new_disk, parent_id=parent_id)


@celery.task()
def get(json_data):
    disk = Disk.get(dir=json_data['dir'], name=json_data['name'])
    return disk.get_desc()


@celery.task()
def get_storage_stat(path):
    ''' Return free disk space avaliable at path in bytes and percent.'''
    s = statvfs(path)
    all_space = s.f_bsize * s.f_blocks
    free_space = s.f_bavail * s.f_frsize
    free_space_percent = 100.0 * free_space / all_space
    return {'free_space': free_space,
            'free_percent': free_space_percent}


@celery.task
def move_to_trash(datastore, disk_name):
    ''' Move path to the trash directory.
    '''
    trash_path = path.join(datastore, trash_directory)
    disk_path = path.join(datastore, disk_name)
    if not path.isdir(trash_path):
        mkdir(trash_path)
    # TODO: trash dir configurable?
    move(disk_path, trash_path)


@celery.task
def recover_from_trash(datastore, disk_name):
    ''' Recover named disk from the trash directory.
    '''
    if path.exists(path.join(datastore, disk_name)):
        return False
    disk_path = path.join(datastore, trash_directory, disk_name)
    # TODO: trash dir configurable?
    move(disk_path, datastore)
    return True


@celery.task
def make_free_space(datastore, percent=10):
    ''' Check for free space on datastore.
        If free space is less than the given percent
        removes oldest files to satisfy the given requirement.
    '''
    trash_path = path.join(datastore, trash_directory)

    def comp(filename):
        try:
            return path.getctime(path.join(trash_path, filename))
        except OSError:
            return 0

    files = sorted(listdir(trash_path), key=comp)
    logger.info("Free space on datastore: %s" %
                get_storage_stat(trash_path).get('free_percent'))
    while get_storage_stat(trash_path).get('free_percent') < percent:
        logger.debug(get_storage_stat(trash_path))
        try:
            f = files.pop(0)
            unlink(path.join(trash_path, f))
            logger.info('Image: %s removed.' % f)
        except IndexError:
            raise Exception("Trash folder is empty.")
    return True
