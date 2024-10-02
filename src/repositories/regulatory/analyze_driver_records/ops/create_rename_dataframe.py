from pathlib import Path
from typing import Optional

import pandas as pd

from dagster import Array, Field, op, OpExecutionContext, String


@op(
    config_schema={
        "paths": Field(
            Array(String), description="Array of paths to nest the result files within"
        )
    }
)
def create_rename_dataframe(
    context: OpExecutionContext, files: list[dict], analysis: list[dict]
) -> list[dict]:
    df = pd.DataFrame.from_records(files)

    df["rename_path"] = pd.Series(None)

    if len(analysis):
        df = df.merge(
            pd.DataFrame.from_records(analysis)[
                ["remote_path", "result", "report_date", "analysis"]
            ],
            how="left",
            on=["remote_path"],
        )

        def rename(remote_path: str, result: Optional[str]) -> str:
            p = Path(remote_path)

            filename = p.stem
            if not pd.isnull(result):
                filename += f"_{result}"
            filename += p.suffix

            return str(p.parent.joinpath(*context.op_config["paths"], filename))

        df["rename_path"] = df.apply(
            lambda x: rename(x.remote_path, x.result), axis="columns"
        )

    return df.to_dict(orient="records")
