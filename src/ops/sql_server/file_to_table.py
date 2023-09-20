from datetime import datetime
from pathlib import Path

import pyarrow as pa

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
    ext = p.suffix

    t: pa.Table

    # Get the table in some form
    if ext == ".csv":
        from pyarrow import csv

        t = csv.read_csv(p)
    elif ext == ".parquet":
        from pyarrow.parquet import read_table

        t = read_table(p)

    reader = t.to_reader()

    sql = f"insert into {table_name} ({','.join(t.schema.names)}) values ({','.join(['?' for x in range(len(t.schema.names))])});"

    context.log.info(f"Will execute '{sql}' for {t.num_rows} rows")
    trace = datetime.now()

    running = True

    while running:
        try:
            batch = reader.read_next_batch()
            for row in batch.to_pylist():
                conn.execute(context, sql, *[v for v in row.values()])
        except StopIteration:
            running = False

    context.log.info(
        f"Executing '{sql}' for {t.num_rows} rows took {datetime.now() - trace}"
    )

    return path
