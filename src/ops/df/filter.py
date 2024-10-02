import pandas as pd

from dagster import Field, OpExecutionContext, Permissive, op


@op(config_schema={"filters": Field(Permissive())})
def filter_dataframe(context: OpExecutionContext, data: list[dict]) -> list[dict]:
    df = pd.DataFrame.from_records(data)

    if len(df) == 0:
        context.log.warning("No records to filter!")
        return data

    for k, v in context.op_config["filters"].items():
        df = df[df[k] == v]

    return df.to_dict(orient="records")
