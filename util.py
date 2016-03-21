import rados
import os


class CephConnection:

    def __init__(self, pool_name, ceph_config=None):

        self.pool_name = pool_name
        self.ceph_config = ceph_config
        self.cluster = None
        self.ioctx = None

    def __enter__(self):
        try:
            if self.ceph_config is None:
                self.ceph_config = os.getenv("CEPH_CONFIG",
                                             "/etc/ceph/ceph.conf")
            self.cluster = rados.Rados(conffile=self.ceph_config)
            self.cluster.connect(timeout=2)
            self.ioctx = self.cluster.open_ioctx(self.pool_name)
        except rados.InterruptedOrTimeoutError as e:
            raise Exception(e)

        return self

    def __exit__(self, type, value, traceback):

        self.ioctx.close()
        self.cluster.shutdown()
