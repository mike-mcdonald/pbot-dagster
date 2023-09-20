from datetime import datetime

from dagster import Field, In, List, OpExecutionContext, Out, String, op

from resources.mssql import MSSqlServerResource


OP_CONFIG = dict(
    config_schema={
        "schema": Field(String),
    },
    ins={
        "table": In(String),
    },
    out=Out(
        String,
        description="The name of the now truncated tables in the specifed schema",
    ),
    required_resource_keys={"sql_server"},
)


@op(**OP_CONFIG)
def truncate_table(context: OpExecutionContext, table: str) -> str:
    conn: MSSqlServerResource = context.resources.sql_server
    schema = context.op_config["schema"]

    context.log.info(f"🚀 Truncating '{table}' in '{schema}'...")
    trace = datetime.now()

    conn.execute(
        context,
        f"truncate table {schema}.{table}",
    )

    context.log.info(
        f"Truncating '{table}' in '{schema}' took {datetime.now() - trace}"
    )

    return table
