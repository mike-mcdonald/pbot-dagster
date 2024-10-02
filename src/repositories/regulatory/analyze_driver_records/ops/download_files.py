from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import pandas as pd

from dagster import (
    Field,
    OpExecutionContext,
    Out,
    Permissive,
    String,
    op,
)

from resources.ssh import SFTPClient


@op(
    config_schema={
        "filter": Field(
            Permissive(),
            description="Mapping of field name and value to filter the DataFrame of files to download",
        )
    },
    required_resource_keys=["ssh_client"],
)
def download_files(context: OpExecutionContext, data: list[dict]) -> list[dict]:
    df: pd.DataFrame = pd.DataFrame.from_records(data)

    for k, v in context.op_config["filter"].items():
        df = df[df[k] == v]

    trace = datetime.now()

    client: SFTPClient = context.resources.ssh_client.connect()

    for row in df[["remote_path", "local_path"]].itertuples(index=False):
        Path(row.local_path).resolve().parent.mkdir(parents=True, exist_ok=True)

        context.log.debug(f"Downloading {row.remote_path} to {row.local_path}...")

        client.get(row.remote_path, row.local_path)

    client.close()

    context.log.info(f"Downloading {len(df)} files took {datetime.now() - trace}.")

    return df.to_dict(orient="records")
