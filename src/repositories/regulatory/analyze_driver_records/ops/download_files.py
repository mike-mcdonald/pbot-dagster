from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import pandas as pd

from paramiko import SFTPClient

from dagster import (
    Field,
    OpExecutionContext,
    Out,
    Permissive,
    String,
    op,
)


@op(
    config_schema={
        "filter": Field(
            Permissive(),
            description="Mapping of field name and value to filter the DataFrame of files to download",
        )
    },
    required_resource_keys=["sftp"],
)
def download_files(context: OpExecutionContext, data: list[dict]) -> list[dict]:
    df: pd.DataFrame = pd.DataFrame.from_records(data)

    if not len(df):
        context.log.warning("No files to download!")
        return []

    for k, v in context.op_config["filter"].items():
        df = df[df[k] == v]

    trace = datetime.now()

    client: SFTPClient = context.resources.sftp.connect()

    for row in df[["remote_path", "local_path"]].itertuples(index=False):
        Path(row.local_path).resolve().parent.mkdir(parents=True, exist_ok=True)

        context.log.debug(f"Downloading {row.remote_path} to {row.local_path}...")

        client.get(row.remote_path, row.local_path)

    client.close()

    context.log.info(f"Downloading {len(df)} files took {datetime.now() - trace}.")

    return df.to_dict(orient="records")
