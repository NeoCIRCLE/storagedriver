import json
import os
import subprocess
import re
import logging
from shutil import move, copy, copyfileobj
from zipfile import ZipFile, is_zipfile
from zlib import decompressobj, MAX_WBITS
from bz2 import BZ2Decompressor

import requests

logger = logging.getLogger(__name__)

re_qemu_img = re.compile(r'(file format: (?P<format>(qcow2|raw))|'
                         r'virtual size: \w+ \((?P<size>[0-9]+) bytes\)|'
                         r'backing file: \S+ \(actual path: (?P<base>\S+)\))$')


class Disk(object):
    ''' Storage driver DISK object.
        Handle qcow2, raw and iso images.
        TYPES, CREATE_TYPES, SNAPSHOT_TYPES are hand managed restrictions.
    '''
    TYPES = ['snapshot', 'normal']
    FORMATS = ['qcow2', 'raw', 'iso']
    CREATE_FORMATS = ['qcow2', 'raw']

    def __init__(self, dir, name, format, type, size, base_name):
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
        self.base_name = base_name

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
            'size': self.size,
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
    def get(cls, dir, name):
        """Create disk from path."""
        path = os.path.realpath(dir + '/' + name)
        output = subprocess.check_output(['qemu-img', 'info', path])

        type = 'normal'
        base = None
        for line in output.split('\n'):
            m = re_qemu_img.search(line)
            if m:
                res = m.groupdict()
                if res.get('format', None) is not None:
                    format = res['format']
                if res.get('size', None) is not None:
                    size = res['size']
                if res.get('base', None) is not None:
                    base = os.path.basename(res['base'])
                    type = 'snapshot'

        return Disk(dir, name, format, size, base, type)

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

    def download(self, task, url, parent_id=None):  # noqa
        """Download image from url."""
        disk_path = self.get_path()
        logger.info("Downloading image from %s to %s", url, disk_path)
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            class AbortException(Exception):
                pass
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
                self.size = os.path.getsize(disk_path)
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

    def merge(self, new_disk):
        """ Merging a new_disk from the actual disk and its base.
        """
        # Check if file already exists
        if os.path.isfile(new_disk.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        if self.format == "iso":
            os.symlink(self.get_path(), new_disk.get_path())
        elif self.base_name:
            cmdline = ['qemu-img',
                       'convert',
                       self.get_path(),
                       '-O', new_disk.format,
                       new_disk.get_path()]
            # Call subprocess
            subprocess.check_output(cmdline)
        else:
            copy(self.get_path(), new_disk.get_path())

    def delete(self):
        """ Delete file. """
        if os.path.isfile(self.get_path()):
            os.unlink(self.get_path())

    @classmethod
    def list(cls, dir):
        """ List all files in <dir> directory."""
        return [cls.get(dir, file) for file in os.listdir(dir)]
