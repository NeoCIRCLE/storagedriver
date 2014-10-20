import json
import os
import subprocess
import logging
import magic
from shutil import move, copyfileobj
from zipfile import ZipFile, is_zipfile
from zlib import decompressobj, MAX_WBITS
from bz2 import BZ2Decompressor
from time import sleep
from hashlib import md5
import re

import requests

logger = logging.getLogger(__name__)

re_qemu_img = re.compile(r'(file format: (?P<format>(qcow2|raw))|'
                         r'virtual size: \w+ \((?P<size>[0-9]+) bytes\)|'
                         r'backing file: \S+ \(actual path: (?P<base>\S+)\))$')


class AbortException(Exception):
    pass


class Disk(object):

    ''' Storage driver DISK object.
        Handle qcow2, raw and iso images.
        TYPES, CREATE_TYPES, SNAPSHOT_TYPES are hand managed restrictions.
    '''
    TYPES = ['snapshot', 'normal']
    FORMATS = ['qcow2', 'raw', 'iso']
    CREATE_FORMATS = ['qcow2', 'raw']

    def __init__(self, dir, name, format, type, size,
                 base_name, actual_size=0):
        # TODO: tests
        self.name = name
        self.dir = os.path.realpath(dir)
        if format not in self.FORMATS:
            raise Exception('Invalid format: %s' % format)
        self.format = format
        if type not in self.TYPES:
            raise Exception('Invalid type: %s' % format)
        self.type = type
        try:
            self.size = int(size)
        except:
            self.size = None
        self.actual_size = actual_size
        self.base_name = base_name

    @property
    def checksum(self, blocksize=65536):
        hash = md5()
        with open(self.get_path(), "rb") as f:
            for block in iter(lambda: f.read(blocksize), ""):
                hash.update(block)
        return hash.hexdigest()

    @classmethod
    def deserialize(cls, desc):
        """Create cls object from JSON."""
        logging.info(desc)
        if isinstance(desc, basestring):
            desc = json.loads(desc)
        return cls(**desc)

    def get_desc(self):
        """Create dict from Disk object."""
        return {
            'name': self.name,
            'dir': self.dir,
            'format': self.format,
            'type': self.type,
            'size': self.size,
            'actual_size': self.actual_size,
            'base_name': self.base_name,
        }

    def get_path(self):
        """Get absolute path for disk."""
        return os.path.realpath(self.dir + '/' + self.name)

    def get_base(self):
        """Get absolute path for disk's base image."""
        return os.path.realpath(self.dir + '/' + self.base_name)

    def __unicode__(self):
        return u'%s %s %s %s' % (self.get_path(), self.format,
                                 self.size, self.get_base())

    @classmethod
    def get_legacy(cls, dir, name):
        ''' Create disk from path
        '''
        path = os.path.realpath(dir + '/' + name)
        output = subprocess.check_output(['qemu-img', 'info', path])

        type = 'normal'
        base_name = None
        for line in output.split('\n'):
            m = re_qemu_img.search(line)
            if m:
                res = m.groupdict()
                if res.get('format', None) is not None:
                    format = res['format']
                if res.get('size', None) is not None:
                    size = float(res['size'])
                if res.get('base', None) is not None:
                    base_name = os.path.basename(res['base'])
                    type = 'snapshot'
        actual_size = size
        return Disk(dir, name, format, type, size, base_name, actual_size)

    @classmethod
    def get_new(cls, dir, name):
        """Create disk from path."""
        path = os.path.realpath(dir + '/' + name)
        output = subprocess.check_output(
            ['qemu-img', 'info', '--output=json', path])
        disk_info = json.loads(output)
        name = name
        format = disk_info.get('format')
        size = disk_info.get('virtual-size')
        actual_size = disk_info.get('actual-size')
        # Check if disk has base (backing-image)
        # Based on backing image determine wether snapshot ot normal image
        base_path = disk_info.get('backing-filename')
        if base_path:
            base_name = os.path.basename(base_path)
            type = 'snapshot'
        else:
            base_name = None
            type = 'normal'
        return Disk(dir, name, format, type, size, base_name, actual_size)

    @classmethod
    def get(cls, dir, name):
        from platform import dist
        if dist()[1] < '14.04':
            return Disk.get_legacy(dir, name)
        else:
            return Disk.get_new(dir, name)

    def create(self):
        """ Creating new image format specified at self.format.
            self.format can be "qcow2-normal"
        """
        # Check if type is avaliable to create
        if self.format not in self.CREATE_FORMATS:
            raise Exception('Invalid format: %s' % self.format)
        if self.type != 'normal':
            raise Exception('Invalid type: %s' % self.format)
        # Check for file if already exist
        if os.path.isfile(self.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        # Build list of Strings as command parameters
        cmdline = ['qemu-img',
                   'create',
                   '-f', self.format,
                   self.get_path(),
                   str(self.size)]
        logging.info("Create file: %s " % cmdline)
        # Call subprocess
        subprocess.check_output(cmdline)

    def check_valid_image(self):
        """Check wether the downloaded image is valid.
        Set the proper type for valid images."""
        format_map = [
            ("qcow", "qcow2-norm"),
            ("iso", "iso"),
            ("x86 boot sector", "iso")
        ]
        with magic.Magic() as m:
            ftype = m.id_filename(self.get_path())
            logger.debug("Downloaded file type is: %s", ftype)
            for file_type, disk_format in format_map:
                if file_type in ftype.lower():
                    self.format = disk_format
                    return True
        return False

    def download(self, task, url, parent_id=None):  # noqa
        """Download image from url."""
        disk_path = self.get_path()
        logger.info("Downloading image from %s to %s", url, disk_path)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            raise Exception("Invalid response status code: %s at %s" %
                            (r.status_code, url))

        if task.is_aborted():
            raise AbortException()
        if parent_id is None:
            parent_id = task.request.id
        chunk_size = 256 * 1024
        ext = url.split('.')[-1].lower()
        if ext == 'gz':
            decompressor = decompressobj(16 + MAX_WBITS)
            # undocumented zlib feature http://stackoverflow.com/a/2424549
        elif ext == 'bz2':
            decompressor = BZ2Decompressor()
        clen = max(int(r.headers.get('content-length', 700000000)), 1)
        percent = 0
        try:
            with open(disk_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if ext in ('gz', 'bz'):
                        chunk = decompressor.decompress(chunk)
                    f.write(chunk)
                    actsize = f.tell()
                    new_percent = min(100, round(actsize * 100.0 / clen))
                    if new_percent > percent:
                        percent = new_percent
                        if not task.is_aborted():
                            task.update_state(
                                task_id=parent_id,
                                state=task.AsyncResult(parent_id).state,
                                meta={'size': actsize, 'percent': percent})
                        else:
                            raise AbortException()
                if ext == 'gz':
                    f.write(decompressor.flush())
                f.flush()
            self.size = Disk.get(self.dir, self.name).size
            logger.debug("Download finished %s (%s bytes)",
                         self.name, self.size)
        except AbortException:
            # Cleanup file:
            os.unlink(disk_path)
            logger.info("Download %s aborted %s removed.",
                        url, disk_path)
        except:
            os.unlink(disk_path)
            logger.error("Download %s failed, %s removed.",
                         url, disk_path)
            raise
        else:
            if ext == 'zip' and is_zipfile(disk_path):
                task.update_state(
                    task_id=parent_id,
                    state=task.AsyncResult(parent_id).state,
                    meta={'size': actsize, 'extracting': 'zip',
                          'percent': 99})
                self.extract_iso_from_zip(disk_path)
            if not self.check_valid_image():
                os.unlink(disk_path)
                raise Exception("Invalid file format. Only qcow and "
                                "iso files are allowed. Image from: %s" % url)

    def extract_iso_from_zip(self, disk_path):
        with ZipFile(disk_path, 'r') as z:
            isos = z.namelist()
            if len(isos) != 1:
                isos = [i for i in isos
                        if i.lower().endswith('.iso')]
            if len(isos) == 1:
                logger.info('Unzipping %s started.', disk_path)
                f = open(disk_path + '~', 'wb')
                zf = z.open(isos[0])
                with zf, f:
                    copyfileobj(zf, f)
                    f.flush()
                move(disk_path + '~', disk_path)
            else:
                logger.info("Extracting %s failed, keeping original.",
                            disk_path)

    def snapshot(self):
        ''' Creating qcow2 snapshot with base image.
        '''
        # Check if snapshot type and qcow2 format matchmatch
        if self.type != 'snapshot':
            raise Exception('Invalid type: %s' % self.type)
        # Check if file already exists
        if os.path.isfile(self.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        # Check if base file exist
        if not os.path.isfile(self.get_base()):
            raise Exception('Image Base does not exists: %s' % self.get_base())
        # Build list of Strings as command parameters
        if self.format == 'iso':
            os.symlink(self.get_base(), self.get_path())
        elif self.format == 'raw':
            raise NotImplemented()
        else:
            cmdline = ['qemu-img',
                       'create',
                       '-b', self.get_base(),
                       '-f', self.format,
                       self.get_path()]
            # Call subprocess
            subprocess.check_output(cmdline)

    def merge_disk_with_base(self, task, new_disk, parent_id=None):
        proc = None
        try:
            cmdline = [
                'qemu-img', 'convert', self.get_path(),
                '-O', new_disk.format, new_disk.get_path()]
            # Call subprocess
            logger.debug(
                "Merging %s into %s.", self.get_path(),
                new_disk.get_path())
            percent = 0
            diff_disk = Disk.get(self.dir, self.name)
            base_disk = Disk.get(self.dir, self.base_name)
            clen = max(base_disk.actual_size + diff_disk.actual_size,
                       diff_disk.size)
            output = new_disk.get_path()
            proc = subprocess.Popen(cmdline)
            while True:
                if proc.poll() is not None:
                    break
                try:
                    actsize = os.path.getsize(output)
                except OSError:
                    actsize = 0
                new_percent = min(100, round(actsize * 100.0 / clen))
                if new_percent > percent:
                    percent = new_percent
                    if not task.is_aborted():
                        task.update_state(
                            task_id=parent_id,
                            state=task.AsyncResult(parent_id).state,
                            meta={'size': actsize, 'percent': percent})
                    else:
                        logger.warning(
                            "Merging new disk %s is aborted by user.",
                            new_disk.get_path())
                        raise AbortException()
                sleep(1)
        except AbortException:
            proc.terminate()
            logger.warning("Aborted merge job, removing %s",
                           new_disk.get_path())
            os.unlink(new_disk.get_path())

        except:
            if proc:
                proc.terminate()
            logger.exception("Unknown error occured, removing %s ",
                             new_disk.get_path())
            os.unlink(new_disk.get_path())
            raise

    def merge_disk_without_base(self, task, new_disk, parent_id=None,
                                length=1024 * 1024):
        try:
            fsrc = open(self.get_path(), 'rb')
            fdst = open(new_disk.get_path(), 'wb')
            clen = self.size
            actsize = 0
            percent = 0
            with fsrc, fdst:
                while True:
                    buf = fsrc.read(length)
                    if not buf:
                        break
                    fdst.write(buf)
                    actsize += len(buf)
                    new_percent = min(100, round(actsize * 100.0 / clen))
                    if new_percent > percent:
                        percent = new_percent
                        if not task.is_aborted():
                            task.update_state(
                                task_id=parent_id,
                                state=task.AsyncResult(parent_id).state,
                                meta={'size': actsize, 'percent': percent})
                        else:
                            logger.warning(
                                "Merging new disk %s is aborted by user.",
                                new_disk.get_path())
                            raise AbortException()
        except AbortException:
            logger.warning("Aborted remove %s", new_disk.get_path())
            os.unlink(new_disk.get_path())
        except:
            logger.exception("Unknown error occured removing %s ",
                             new_disk.get_path())
            os.unlink(new_disk.get_path())
            raise

    def merge(self, task, new_disk, parent_id=None):
        """ Merging a new_disk from the actual disk and its base.
        """

        if task.is_aborted():
            raise AbortException()

        # Check if file already exists
        if os.path.isfile(new_disk.get_path()):
            raise Exception('File already exists: %s' % self.get_path())

        if self.format == "iso":
            os.symlink(self.get_path(), new_disk.get_path())
        elif self.base_name:
            self.merge_disk_with_base(task, new_disk, parent_id)
        else:
            self.merge_disk_without_base(task, new_disk, parent_id)

    def delete(self):
        """ Delete file. """
        if os.path.isfile(self.get_path()):
            os.unlink(self.get_path())

    @classmethod
    def list(cls, dir):
        """ List all files in <dir> directory."""
        return [cls.get(dir, file) for file in os.listdir(dir)]
