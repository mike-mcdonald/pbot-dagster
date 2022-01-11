from pathlib import Path
from typing import Union

from dagster import resource
from dagster.core.types.dagster_type import List

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileProperties

from resources.base import BaseResource


class AzureDataLakeGen2Resource(BaseResource):
    def __init__(
        self,
        azure_data_lake_gen2_conn_id="azure_data_lake_gen2",
    ):
        self.azure_data_lake_gen2_conn_id = azure_data_lake_gen2_conn_id
        self.client = self.get_connection()

    def get_connection(self) -> DataLakeServiceClient:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        connection = super().get_connection(self.azure_data_lake_gen2_conn_id)

        return DataLakeServiceClient(
            account_url=f"https://{connection.host}.dfs.core.windows.net/",
            credential=ClientSecretCredential(
                connection.extra_dejson.get("tenant_id"),
                connection.login,
                connection.password,
            ),
        )

    def upload_file(
        self,
        file_system: str,
        local_path: Union[str, Path],
        remote_path: Union[str, Path],
    ) -> FileProperties:
        """
        Copy a file at {local_path} to {remote_path} on {file_system} of this storage account.
        Overwrites by default if file with same remote path/name exists.

        :param file_system:
            The file system. Would represent https://{file_system}@{storage_account}.dfs.windows.net
        :type file_system: str
        :param local_path:
            The path to the file on this system. Try to use absolute path, current working directory might be hard to know at runtime.
            Be careful, and try to keep file size reasonable by splitting large datasets. This will read the file into a byte array in memory before sending.
        :type local_path: str
        :param remote_path:
            The path on {file_system} to copy this file to. Include file name. Translates to https://{file_system}@{storage_account}.dfs.windows.net/{remote_path}
        :type remote_path: str
        :return: An instance of FileProperties
        :rtype:FileProperties
                :start-after: [START get_file_properties]
                :end-before: [END get_file_properties]
                :language: python
                :dedent: 4
                :caption: Getting the properties for a file.
        """

        local_path = Path(local_path)
        remote_path = Path(remote_path)

        if local_path.is_dir():
            raise ValueError(
                f"{local_path} is a directory! Call `upload_directory` instead."
            )

        file_client = self.client.get_directory_client(
            file_system, str(remote_path.parent)
        ).create_file(remote_path.name)

        with local_path.open("rb") as f:
            file_client.upload_data(f, overwrite=True)

        return file_client.get_file_properties()

    def upload_directory(
        self,
        file_system: str,
        local_path: Union[str, Path],
        remote_path: Union[str, Path],
    ) -> List[FileProperties]:
        local_path = Path(local_path)
        remote_path = Path(remote_path)

        properties = []

        for p in local_path.iterdir():
            if p.is_dir():
                properties.extend(
                    self.upload_directory(file_system, p, remote_path / p.name)
                )
            else:
                properties.append(
                    self.upload_file(file_system, p, remote_path / p.name)
                )

        return properties


@resource(config_schema={"azure_data_lake_gen2_conn_id": str})
def adls2_resource(init_context):
    return AzureDataLakeGen2Resource(
        azure_data_lake_gen2_conn_id=init_context.resource_config[
            "azure_data_lake_gen2_conn_id"
        ]
    )
