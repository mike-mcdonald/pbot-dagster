from datetime import datetime
from pathlib import Path

import pyarrow as pa

from pyarrow.parquet import read_table

from dagster import (
    Field,
    In,
    OpExecutionContext,
    Out,
    String,
    op,
)

from resources.mssql import MSSqlServerResource


@op(
    config_schema={
        "schema": Field(String),
    },
    required_resource_keys={"sql_server"},
    ins={
        "path": In(String),
        "table": In(String),
    },
    out=Out(String, "The file of data inserted into the table"),
)
def file_to_table(context: OpExecutionContext, path: str, table: str):
    table_name = f"[{context.op_config['schema']}].[{table}]"

    conn: MSSqlServerResource = context.resources.sql_server

    # read path as Path
    p = Path(path)

    t: pa.Table = read_table(p)

    sql = f"insert into {table_name} ({','.join(t.schema.names)}) values ({','.join(['?' for x in range(len(t.schema.names))])});"

    context.log.info(f"Will execute '{sql}' for {t.num_rows} rows")
    trace = datetime.now()

    for batch in t.to_batches():
        for row in zip(*[v for v in batch.to_pydict().values()]):
            conn.execute(context, sql, *row)

    context.log.info(
        f"Executing '{sql}' for {t.num_rows} rows took {datetime.now() - trace}"
    )

    return path
