import os

from pathlib import Path

import pandas as pd

from dagster import Field, OpExecutionContext, String, op


@op(config_schema={"path": Field(String, description="Path to write output to")})
def write_dataframe(context: OpExecutionContext, data: list[dict]) -> str:
    path = Path(context.op_config["path"])

    path.parent.mkdir(parents=True, exist_ok=True)

    path = str(path.resolve())

    df = pd.DataFrame.from_records(data)

    writer = {".csv": df.to_csv, ".json": df.to_json, ".parquet": df.to_parquet}.get(
        os.path.splitext(path)[1], df.to_parquet
    )

    writer(path, index=False)

    return path
