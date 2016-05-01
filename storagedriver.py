from disk import Disk, CephDisk
from util import CephConnection
from storagecelery import celery
import os
from os import unlink, statvfs, listdir
from celery.contrib.abortable import AbortableTask
import logging

import rbd

logger = logging.getLogger(__name__)

trash_directory = "trash"


@celery.task()
def list(data_store_type, dir):
    cls = CephDisk if data_store_type == "ceph_block" else Disk
    return [d.get_desc() for d in cls.list(dir)]


@celery.task()
def list_files(data_store_type, dir):

    if data_store_type == "ceph_block":
        with CephConnection(str(dir)) as conn:
            rbd_inst = rbd.RBD()
            return rbd_inst.list(conn.ioctx)
    else:
        return [l for l in listdir(dir) if
                os.path.isfile(os.path.join(dir, l))]


@celery.task()
def create(disk_desc):
    cls = CephDisk if disk_desc["data_store_type"] == "ceph_block" else Disk
    disk = cls.deserialize(disk_desc)
    disk.create()


class download(AbortableTask):
    time_limit = 18000  # TODO: calculate proper value it's 5h now

    def run(self, **kwargs):
        disk_desc = kwargs['disk']
        url = kwargs['url']
        parent_id = kwargs.get("parent_id", None)
        c = CephDisk if disk_desc["data_store_type"] == "ceph_block" else Disk
        disk = c.deserialize(disk_desc)
        disk.download(self, url, parent_id)
        return {'size': disk.size,
                'type': disk.format,
                'checksum': disk.checksum, }


@celery.task()
def delete(disk_desc):
    cls = CephDisk if disk_desc["data_store_type"] == "ceph_block" else Disk
    disk = cls.deserialize(disk_desc)
    disk.delete()


@celery.task()
def delete_dump(data_store_type, dir, filename):
    if data_store_type == "ceph_block":
        with CephConnection(str(dir)) as conn:
            rbd_inst = rbd.RBD()
            rbd_inst.remove(conn.ioctx, str(filename))
    else:
        disk_path = dir + "/" + filename
        if disk_path.endswith(".dump") and os.path.isfile(disk_path):
            unlink(disk_path)


@celery.task()
def snapshot(disk_desc):
    cls = CephDisk if disk_desc["data_store_type"] == "ceph_block" else Disk
    disk = cls.deserialize(disk_desc)
    disk.snapshot()


class merge(AbortableTask):
    time_limit = 18000

    def run(self, **kwargs):
        old_json = kwargs['old_json']
        new_json = kwargs['new_json']
        parent_id = kwargs.get("parent_id", None)
        cls = CephDisk if old_json["data_store_type"] == "ceph_block" else Disk
        disk = cls.deserialize(old_json)
        new_disk = cls.deserialize(new_json)
        disk.merge(self, new_disk, parent_id=parent_id)


@celery.task()
def get(disk_desc):
    disk = None
    dir = disk_desc['dir']

    if disk_desc["data_store_type"] == "ceph_block":
        with CephConnection(dir) as conn:
            disk = CephDisk.get(conn.ioctx, pool_name=dir,
                                name=disk_desc['name'])
    else:
        disk = Disk.get(dir=dir, name=disk_desc['name'])

    return disk.get_desc()


@celery.task()
def get_storage_stat(data_store_type, path):
    ''' Return free disk space avaliable at path in bytes and percent.'''
    all_space = 1
    free_space = 0
    if data_store_type == "ceph_block":
        with CephConnection(str(path)) as conn:
            stat = conn.cluster.get_cluster_stats()
            all_space = stat["kb"]
            free_space = stat["kb_avail"]
    else:
        s = statvfs(path)
        all_space = s.f_bsize * s.f_blocks
        free_space = s.f_bavail * s.f_frsize

    free_space_percent = 100.0 * free_space / all_space
    return {'free_space': free_space,
            'free_percent': free_space_percent}


@celery.task
def exists(data_store_type, path, disk_name):
    ''' Recover named disk from the trash directory.
    '''
    if data_store_type == "ceph_block":
        try:
            with CephConnection(str(path)) as conn:
                with rbd.Image(conn.ioctx, str(disk_name)):
                    pass
        except rbd.ImageNotFound:
            return False
        else:
            return True
    elif os.path.exists(os.path.join(path, disk_name)):
        return True

    return False


@celery.task
def make_free_space(data_store_type, path, deletable_disks, percent=10):
    ''' Check for free space on datastore.
        If free space is less than the given percent
        removes oldest files to satisfy the given requirement.
    '''
    ds_type = data_store_type
    logger.info("Free space on datastore: %s" %
                get_storage_stat(ds_type, path).get('free_percent'))
    while get_storage_stat(ds_type, path).get('free_percent') < percent:
        logger.debug(get_storage_stat(ds_type, path))
        try:
            f = deletable_disks.pop(0)
            if ds_type == "ceph_block":
                with CephConnection(str(path)) as conn:
                    rbd_inst = rbd.RBD()
                    with rbd.Image(conn.ioctx, str(f)) as image:
                        for snapshot in image.list_snaps():
                            name = snapshot["name"]
                            image.unprotect_snap(name)
                            image.remove_snap(name)
                    rbd_inst.remove(conn.ioctx, str(f))
            else:
                unlink(os.path.join(path, f))
            logger.info('Image: %s removed.' % f)
        except IndexError:
            logger.warning("Has no deletable disk.")
            return False
    return True
