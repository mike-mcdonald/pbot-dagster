import csv
import decimal

from datetime import date, datetime, time
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple, Union

import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq
import pyodbc

from dagster import (
    Array,
    Field,
    In,
    Int,
    OpExecutionContext,
    Out,
    Permissive,
    String,
    op,
)

from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource


def __op(
    context: OpExecutionContext,
    table: str,
    writerClass: Union[pc.CSVWriter, pq.ParquetWriter],
):
    schema = context.op_config["schema"]
    batch_size = context.op_config["batch_size"]
    exclusions = context.op_config["exclude"]

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

    cursor = conn.execute(context, f"select * from [{schema}].[{table}]")

    context.log.info(f"🚀 Started processing {schema}.{table} to {local_path}...")
    trace = datetime.now()

    # Initial fetch to build table metadata
    rows = cursor.fetchmany(batch_size)

    ptype_map = {
        bool: lambda *_: pa.bool_(),
        bytes: lambda *_: pa.binary(),
        decimal.Decimal: lambda p, s: pa.decimal128(p, s),
        date: lambda *_: pa.date64(),
        datetime: lambda *_: pa.timestamp("us"),
        int: lambda *_: pa.int64(),
        float: lambda *_: pa.float64(),
        str: lambda *_: pa.string(),
        time: lambda *_: pa.time64(),
    }

    fields = []

    for cd in cursor.description:
        name, type_code, display_size, internal_size, precision, scale, null_ok = cd

        if name not in exclusions:
            fields.append(
                pa.field(name, ptype_map.get(type_code)(precision, scale), null_ok)
            )

    schema = pa.schema(fields)

    with writerClass(str(local_path.resolve()), schema) as writer:
        while len(rows) > 0:
            arrays = []
            for i, n in enumerate(cursor.description):
                if n[0] not in exclusions:
                    arrays.append(pa.array([r[i] for r in rows]))

            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

            writer.write_batch(batch)

            rows = cursor.fetchmany(batch_size)

    context.log.info(f"⌚ Processing {schema}.{table} took {datetime.now() - trace}")

    return str(local_path.resolve())


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
        "exclude": Field(
            Array(String),
            description="List of column names to exclude from processing.",
            default_value=[],
            is_required=False,
        ),
    },
    ins={
        "table": In(String),
    },
    out=Out(String, "The path to the file created by this operation"),
    required_resource_keys={"sql_server"},
)


@op(**OP_CONFIG)
def table_to_csv(context: OpExecutionContext, table: str) -> str:
    return __op(context, table, pc.CSVWriter)


@op(**OP_CONFIG)
def table_to_parquet(context: OpExecutionContext, table: str) -> str:
    return __op(context, table, pq.ParquetWriter)
