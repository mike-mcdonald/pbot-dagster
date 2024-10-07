import pandas as pd

from dagster import (
    Field,
    OpExecutionContext,
    String,
    op,
)


@op()
def combine_dataframes(
    context: OpExecutionContext, dataframes: list[list[dict]]
) -> list[dict]:
    context.log.info(f"Combining {len(dataframes)} DataFrames...")

    df: pd.DataFrame = pd.concat([pd.DataFrame.from_records(f) for f in dataframes])

    context.log.info(f"Combined DataFrame has {len(df)} rows.")

    return df.to_dict(orient="records")
