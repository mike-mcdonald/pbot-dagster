from contextlib import contextmanager
from pathlib import Path
from typing import Union

from dagster import resource


from resources.base import BaseResource


class FileShareResource(BaseResource):
    def __init__(
        self,
        conn_id="fs_default",
    ):
        self.conn_id = conn_id
        self.client = self.get_connection()

    def get_connection(self):
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        return super().get_connection(self.conn_id)

    def __args(self):
        args = ["net", "use", self.client.host]

        if self.client.password:
            args.append(self.client.password)

        if self.client.login:
            args.append(f"/user:{self.client.login}")

        return args

    def _open_share(self):
        import subprocess

        subprocess.run([*self.__args(), "/persistent:no"])

    def _close_share(self):
        import subprocess

        subprocess.run([*self.__args(), "/delete"])

    def upload(
        self,
        local_path: Union[str, Path],
        remote_path: Union[str, Path],
    ):
        import shutil

        path = Path(self.client.host) / remote_path

        path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy(local_path, path)


@resource(config_schema={"conn_id": str})
@contextmanager
def fileshare_resource(init_context):
    resource = FileShareResource(conn_id=init_context.resource_config["conn_id"])

    try:
        resource._open_share()
        yield resource
    finally:
        resource._close_share()
