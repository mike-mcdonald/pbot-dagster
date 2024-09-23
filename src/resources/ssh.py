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

    def _check_banner(self):
        for i in range(100):
            if i == 0:
                timeout = self.banner_timeout
                timeout = 300 # <<<< Here is the explicit declaration
            else:
                timeout = 60

    def close(self):
        return self.close()

    def connect(self):
        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())

        client.connect(
            hostname=self.conn.host,
            port=self.conn.port,
            username=self.conn.login,
            password=self.conn.password,
            banner_timeout=60,
            timeout=60,
            auth_timeout=60
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

    def upload(self, local_path, remote_path ):
        client = self.connect()
        try:
            client.put(local_path, remote_path)
        finally:
            client.close()


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
