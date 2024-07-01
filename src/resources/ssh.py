from contextlib import contextmanager
from dagster import OpExecutionContext, resource
from paramiko import  AutoAddPolicy,SFTPClient,SSHClient
from resources.base import BaseResource
from textwrap import dedent
from typing import List


class SSHClientResource(BaseResource):

    def __init__(
        self,
        conn_id,
    ): 
        self.conn = self.get_connection(conn_id)
        self.client = self.get_sshClient()  
            

    def get_sshClient(self) -> SSHClient:

        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        ssh_client.connect(hostname=self.conn.host,port=self.conn.port,username=self.conn.login,password=self.conn.password)
        return ssh_client


    def get_sftpClient(self) -> SFTPClient:
        return self.client.open_sftp()
    
    def close(self) -> SFTPClient:
        return self.client.close()

@resource(config_schema={
    "conn_id": str,
    })
@contextmanager
def ssh_resource(init_context):
    ssh_client  = SSHClientResource(
        conn_id=init_context.resource_config["conn_id"],
    )

    try:
        yield ssh_client
    finally:
        ssh_client.close()