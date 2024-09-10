from contextlib import contextmanager
from dagster import OpExecutionContext, resource
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from resources.base import BaseResource
from textwrap import dedent
from typing import List


class SSHClientResource(BaseResource):
    def __init__(
        self,
        conn_id,
    ):
        self.conn = self.get_connection(conn_id)

    def connect(self):
        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())

        client.connect(
            hostname=self.conn.host,
            port=self.conn.port,
            username=self.conn.login,
            password=self.conn.password,
        )

        return client.open_sftp()

    def download(self, remote_path, local_path):
        client = self.connect()
        try:
            client.get(remote_path, local_path)
        finally:
            client.close()

    def list(self, path):
        client = self.connect()
        try:
            # do work
            return client.listdir(path)
        finally:
            client.close()

    def get_sftpClient(self) -> SFTPClient:
        return self.client.open_sftp()

    def close(self) -> SFTPClient:
        return self.client.close()


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
