from contextlib import contextmanager
from dagster import resource
from paramiko import AutoAddPolicy, SSHClient
from resources.base import BaseResource


class SSHClientResource(BaseResource):
    def __init__(
        self,
        conn_id,
    ):
        self.conn = self.get_connection(conn_id)

    def connect(self, **kwargs):
        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())

        client.connect(
            hostname=self.conn.host,
            port=self.conn.port,
            username=self.conn.login,
            password=self.conn.password,
            **kwargs
        )

        return client.open_sftp()

    def download(self, remote_path, local_path):
        with self.connect() as client:
            client.get(remote_path, local_path)

    def list(self, path):
        with self.connect() as client:
            return [f for f in client.listdir_iter(path)]

    def list_iter(self, path):
        with self.connect() as client:
            for f in client.listdir_iter(path):
                yield f

    def move(self, from_path: str, to_path: str):
        with self.connect() as client:
            client.rename(from_path, to_path)

    def put(self, local_path: str, remote_path: str):
        with self.connect() as client:
            client.put(local_path, remote_path)

    def remove(self, path):
        with self.connect() as client:
            client.remove(path)


@resource(
    config_schema={
        "conn_id": str,
    }
)
@contextmanager
def ssh_resource(init_context):
    yield SSHClientResource(
        conn_id=init_context.resource_config["conn_id"],
    )
