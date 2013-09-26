import os
import subprocess
import re

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
        self.size = int(size)
        self.base_name = base_name

    @classmethod
    def deserialize(cls, desc):
        return cls(**desc)

    def get_desc(self):
        return {
            'name': self.name,
            'dir': self.dir,
            'format': self.format,
            'size': self.size,
            'base_name': self.base_name,
        }

    def get_path(self):
        return os.path.realpath(self.dir + '/' + self.name)

    def get_base(self):
        return os.path.realpath(self.dir + '/' + self.base_name)

    def __unicode__(self):
        return u'%s %s %s %s' % (self.get_path(), self.format,
                                 self.size, self.get_base())

    @classmethod
    def get(cls, dir, name):
        ''' Create disk from path
        '''
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
        ''' Creating new image format specified at self.format.
            self.format van be "qcow2-normal"
        '''
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
                   str(self.size)]
        # Call subprocess
        subprocess.check_output(cmdline)

    def snapshot(self):
        ''' Creating qcow2 snapshot with base image.
        '''
        # Check if snapshot type and qcow2 format matchmatch
        if self.format != 'qcow2':
            raise Exception('Invalid format: %s' % self.format)
        if self.type != 'snapshot':
            raise Exception('Invalid type: %s' % self.format)
        # Check if file already exists
        if os.path.isfile(self.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        # Check if base file exist
        if not os.path.isfile(self.get_base()):
            raise Exception('Image Base does not exists: %s' % self.get_base())
        # Build list of Strings as command parameters
        cmdline = ['qemu-img',
                   'create',
                   '-b', self.get_base(),
                   '-f', self.format,
                   self.get_path()]
        # Call subprocess
        subprocess.check_output(cmdline)

    def merge(self, new_disk):
        ''' Merging a new_disk from the actual disk and its base.
        '''
        # Check if snapshot type match
        if self.format != 'qcow2':
            raise Exception('Invalid format: %s' % self.format)
        # Check if file already exists
        if os.path.isfile(new_disk.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        # Check if base file exist
        if not os.path.isfile(self.get_path()):
            raise Exception('Original image does not exists: %s'
                            % self.get_base())
        cmdline = ['qemu-img',
                   'convert',
                   self.get_path(),
                   '-O', new_disk.format,
                   new_disk.get_path()]
        # Call subprocess
        subprocess.check_output(cmdline)

    def delete(self):
        ''' Delete file
        '''
        if os.path.isfile(self.get_path()):
            os.unlink(self.get_path())

    @classmethod
    def list(cls, dir):
        return [cls.get(dir, file) for file in os.listdir(dir)]
