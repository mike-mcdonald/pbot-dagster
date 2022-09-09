import csv
import decimal

from datetime import date, datetime, time
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

from dagster import Field, In, Int, OpExecutionContext, Out, Permissive, String, op

from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource


def __bootstrap(context: OpExecutionContext, table: str):
    schema = context.op_config["schema"]
    batch_size = context.op_config["batch_size"]

    substitutions = {"schema": schema, "table": table}
    if "substitutions" in context.op_config:
        substitutions = dict(**substitutions, **context.op_config["substitutions"])

    local_path = Path(
        apply_substitutions(
            template_string=context.op_config["path"],
            substitutions=substitutions,
            context=context,
        )
    ).resolve()

    local_path.parent.resolve().mkdir(parents=True, exist_ok=True)

    conn: MSSqlServerResource = context.resources.sql_server

    cursor: pyodbc.Cursor = conn.execute(context, f"select * from [{schema}].[{table}]")

    return schema, batch_size, cursor, local_path


def __description_to_schema(description: Tuple[Tuple]):
    ptype_map = {
        bool: lambda *_: pa.bool_(),
        bytes: lambda *_: pa.binary(),
        decimal.Decimal: lambda p, s: pa.decimal128(p, s),
        date: lambda *_: pa.date64(),
        datetime: lambda *_: pa.timestamp("ms"),
        int: lambda *_: pa.int64(),
        float: lambda *_: pa.float64(),
        str: lambda *_: pa.string(),
        time: lambda *_: pa.time64(),
    }

    fields = []

    for cd in description:
        name, type_code, display_size, internal_size, precision, scale, null_ok = cd

        fields.append(
            pa.field(name, ptype_map.get(type_code)(precision, scale), null_ok)
        )

    return pa.schema(fields)


def __rows_to_recordbatch(rows: List[pyodbc.Row], schema: pa.Schema) -> pa.RecordBatch:
    arrays = []

    for i in range(len(schema.names)):
        arrays.append(pa.array([r[i] for r in rows]))

    return pa.RecordBatch.from_arrays(arrays, schema=schema)


OP_CONFIG = dict(
    config_schema={
        "schema": Field(String),
        "path": Field(
            String,
            description=dedent(
                """Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext."""
            ).strip(),
        ),
        "substitutions": Field(
            Permissive(),
            description="Subsitution mapping for substituting in `path`",
            is_required=False,
        ),
        "batch_size": Field(Int, default_value=1000, is_required=False),
    },
    ins={
        "table": In(String),
    },
    out=Out(String, "The path to the file created by this operation"),
    required_resource_keys={"sql_server"},
)


@op(**OP_CONFIG)
def table_to_csv(context: OpExecutionContext, table: str) -> str:
    schema, batch_size, cursor, local_path = __bootstrap(context, table)

    context.log.info(f"🚀 Started processing {schema}.{table} to {local_path}...")
    trace = datetime.now()

    with local_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Initial fetch to build table metadata
        rows = cursor.fetchmany(batch_size)

        # Write header
        writer.writerow([x[0] for x in cursor.description])

        while len(rows) > 0:
            writer.writerows(rows)
            rows = cursor.fetchmany(batch_size)

    context.log.info(f"⌚ Processing {schema}.{table} took {datetime.now() - trace}")

    return str(local_path.resolve())


@op(**OP_CONFIG)
def table_to_parquet(context: OpExecutionContext, table: str) -> str:
    schema, batch_size, cursor, local_path = __bootstrap(context, table)

    context.log.info(f"🚀 Started processing {schema}.{table} to {local_path}...")
    trace = datetime.now()

    # Initial fetch to build table metadata
    rows = cursor.fetchmany(batch_size)

    schema = __description_to_schema(cursor.description)

    with pq.ParquetWriter(str(local_path.resolve()), schema) as writer:
        while len(rows) > 0:
            writer.write_batch(__rows_to_recordbatch(rows, schema))
            rows = cursor.fetchmany(batch_size)

    context.log.info(f"⌚ Processing {schema}.{table} took {datetime.now() - trace}")

    return str(local_path.resolve())
