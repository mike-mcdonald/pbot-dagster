import os

from datetime import datetime
from pathlib import Path

from dagster import Field, OpExecutionContext, op, String

from resources.ssh import SSHClientResource

COMMON_CONFIG = dict(
    config_schema={
        "path": Field(String, description="Local path to download files to")
    },
    required_resource_keys=["sftp"],
)


def _ensure_dir(path: os.PathLike):
    Path(path).mkdir(parents=True, exist_ok=True)


@op(**COMMON_CONFIG)
def download(context: OpExecutionContext, file: str) -> str:
    trace = datetime.now()

    _ensure_dir(context.op_config["path"])

    sftp: SSHClientResource = context.resources.sftp

    path = os.path.join(context.op_config["path"], os.path.basename(file))

    sftp.download(file, path)

    context.log.info(f"Downloaded file in {datetime.now() - trace}.")

    return path


@op(**COMMON_CONFIG)
def download_list(context: OpExecutionContext, files: list[str]) -> list[str]:
    trace = datetime.now()

    _ensure_dir(context.op_config["path"])

    sftp: SSHClientResource = context.resources.sftp

    context.log.info(
        f"Will download {len(files)} files to '{context.op_config['path']}'..."
    )

    local = []

    with sftp.connect() as client:
        for file in files:
            path = os.path.join(context.op_config["path"], os.path.basename(file))
            client.get(file, path)
            local.append(path)

    context.log.info(f"Downloaded files in {datetime.now() - trace}.")

    return local
