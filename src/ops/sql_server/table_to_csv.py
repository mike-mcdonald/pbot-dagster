import csv

from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pyodbc

from dagster import Field, In, Int, OpExecutionContext, Out, Permissive, String, op

from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource


@op(
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
def table_to_csv(context: OpExecutionContext, table: str) -> str:
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
