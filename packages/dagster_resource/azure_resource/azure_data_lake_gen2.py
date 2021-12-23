from pathlib import Path

from dagster import resource

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from conn_manager.conn import get_conn


class AzureDataLakeGen2Resource():
    def __init__(
        self,
        azure_data_lake_gen2_conn_id="azure_data_lake_gen2",
    ):
        self.azure_data_lake_gen2_conn_id = azure_data_lake_gen2_conn_id

    def get_connection(self) -> DataLakeServiceClient:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        self.connection = get_conn(
            self.azure_data_lake_gen2_conn_id)

        return DataLakeServiceClient(
            account_url=f"https://{self.connection.get('host')}.dfs.core.windows.net/",
            credential=ClientSecretCredential(
                self.connection.get("extra").get(
                    "tenant_id"
                ),
                self.connection.get("login"),
                self.connection.get("password"),
            ),
        )

    def upload_file(self, file_system: str, local_path: str, remote_path: str):
        # type: (...) -> None
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

        path = Path(local_path)

        remote_path = remote_path.rstrip("/").lstrip("/")
        dir_name = remote_path.split("/")[:-1]
        dir_name = "/".join(dir_name)
        file_name = remote_path.split("/")[-1]

        file_client = (
            self.get_connection()
            .get_directory_client(file_system, dir_name)
            .create_file(file_name)
        )

        files: list(Path) = []

        if path.is_dir():
            files = [x for x in path.iterdir() if x.is_file()]
        else:
            files = [path]

        for file in files:
            with file.open("rb") as f:
                file_client.upload_data(f, overwrite=True)

        return file_client.get_file_properties()


@resource(config_schema={"azure_data_lake_gen2_conn_id": str})
def azure_pudl_resource(init_context):
    conn_id = init_context.resource_config["azure_data_lake_gen2_conn_id"]
    return AzureDataLakeGen2Resource(conn_id)
