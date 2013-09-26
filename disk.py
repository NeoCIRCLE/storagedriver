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
    TYPES = [('qcow2-norm', 'qcow2 normal'), ('qcow2-snap', 'qcow2 snapshot'),
             ('iso', 'iso'), ('raw-ro', 'raw read-only'), ('raw-rw', 'raw')]

    CREATE_TYPES = [('qcow2-norm', 'qcow2'), ('raw-ro', 'raw'),
                    ('raw-rw', 'raw')]
    SNAPSHOT_TYPES = [('qcow2-snap', 'qcow2')]

    def __init__(self, dir, name, format, size, base_name):
        # TODO: tests
        self.name = name
        self.dir = os.path.realpath(dir)
        if format not in [k[0] for k in self.TYPES]:
            raise Exception('Invalid format: %s' % format)
        self.format = format
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
            'type': self.type,
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
        if os.path.isfile(self.get_path()):
            raise Exception('File already exists: %s' % self.get_path())
        cmdline = ['qemu-img',
                   'create',
                   '-f', self.format]
        if self.type == 'snapshot':
            cmdline.append('-b')
            cmdline.append(self.get_base())
        cmdline.append(self.get_path())
        if self.type != 'snapshot':
            cmdline.append(str(self.size))
        print ' '.join(cmdline)
        subprocess.check_output(cmdline)

    def delete(self):
        if os.path.isfile(self.get_path()):
            os.unlink(self.get_path())

    @classmethod
    def list(cls, dir):
        return [cls.get(dir, file) for file in os.listdir(dir)]
