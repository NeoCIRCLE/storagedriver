import rados
import os


class CephConfig:

    def __init__(self, user=None, config_path=None, keyring_path=None):

        self.user = user or "admin"
        self.config_path = (
            config_path or os.getenv("CEPH_CONFIG", "/etc/ceph/ceph.conf"))
        default_keyring = "/etc/ceph/ceph.client.%s.keyring" % self.user
        self.keyring_path = (
            keyring_path or os.getenv("CEPH_KEYRING", default_keyring))

    def cmd_args(self):
        return ["--keyring", self.keyring_path,
                "--id", self.user,
                "--conf", self.config_path]


class CephConnection:

    def __init__(self, pool_name, conf=None, **kwargs):

        self.pool_name = pool_name
        self.conf = conf or CephConfig(**kwargs)
        self.cluster = None
        self.ioctx = None

    def __enter__(self):
        try:
            self.cluster = rados.Rados(
                conffile=self.conf.config_path,
                conf=dict(keyring=self.conf.keyring_path))
            timeout = os.getenv("CEPH_TIMEOUT", 2)
            self.cluster.connect(timeout=timeout)
            self.ioctx = self.cluster.open_ioctx(self.pool_name)
        except rados.InterruptedOrTimeoutError as e:
            raise Exception(e)

        return self

    def __exit__(self, type, value, traceback):

        self.ioctx.close()
        self.cluster.shutdown()
