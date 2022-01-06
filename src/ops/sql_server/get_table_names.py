from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    String,
    OpExecutionContext,
    op,
)

from resources.mssql import MSSqlServerResource


@op(
    config_schema={"schema": Field(String, default_value="dbo")},
    out=DynamicOut(String, description="The names of tables in the specifed schema"),
    required_resource_keys={"sql_server"},
)
def get_table_names(context: OpExecutionContext) -> str:
    conn: MSSqlServerResource = context.resources.sql_server
    schema = context.op_config["schema"]

    for name in conn.get_tables(schema):
        yield DynamicOutput(
            value=name,
            # create a mapping key from the file name
            mapping_key=f"[{schema}].[{name}]".replace(".", "_")
            .replace("-", "_")
            .lower(),
        )
