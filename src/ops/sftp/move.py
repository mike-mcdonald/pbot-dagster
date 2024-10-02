import os

from datetime import datetime

from paramiko import SFTPClient

from dagster import (
    Field,
    OpExecutionContext,
    op,
    String,
)

from resources.ssh import SSHClientResource

COMMON_CONFIG = dict(
    config_schema={
        "path": Field(
            String, description="Full path to directory where file will move to."
        )
    },
    required_resource_keys=["sftp"],
)


@op(**COMMON_CONFIG)
def move(context: OpExecutionContext, file: str) -> str:
    resource: SSHClientResource = context.resources.sftp
    new_path = os.path.join(context.op_config["path"], os.path.basename(file))

    context.log.info(f"Will move '{file}' to '{new_path}'...")
    trace = datetime.now()

    with resource.connect() as client:
        client.rename(file, new_path)

    context.log.info(f"Moved file in {datetime.now() - trace}.")

    return new_path
