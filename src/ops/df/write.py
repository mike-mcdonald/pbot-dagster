import pandas as pd

from dagster import Field, OpExecutionContext, String, op


@op(config_schema={"path": Field(String, description="Path to write output to")})
def write_dataframe(context: OpExecutionContext, data: list[dict]) -> str:
    pd.DataFrame.from_records(data).to_parquet(context.op_config["path"])
    return context.op_config["path"]
