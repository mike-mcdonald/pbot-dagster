import re

from dagster import (
    Array,
    DynamicOut,
    DynamicOutput,
    Field,
    List,
    String,
    OpExecutionContext,
    Out,
    op,
)

from resources.mssql import MSSqlServerResource


def __get_table_names(context: OpExecutionContext) -> list[str]:
    conn: MSSqlServerResource = context.resources.sql_server
    schema = context.op_config["schema"]
    exclusions = context.op_config["exclude"]

    context.log.info(f"🚀 Retrieving tables in '{schema}' schema...")

    tables = conn.get_tables(context, schema)

    context.log.info(f"👍 Retrieved tables {tables}")

    if exclusions:
        context.log.info(f"Excluding some tables based on '{exclusions}'...")
        for exclusion in exclusions:
            exclusion = re.compile(exclusion)
            tables = [t for t in tables if not exclusion.match(t)]

    return tables


@op(
    config_schema={
        "schema": Field(String, default_value="dbo"),
        "exclude": Field(
            Array(String),
            description="List of tables names to exclude from processing. Values will be parsed as regular expressions.",
            default_value=[],
            is_required=False,
        ),
    },
    out=Out(List[String], description="The names of tables in the specifed schema"),
    required_resource_keys={"sql_server"},
)
def get_table_names(context: OpExecutionContext) -> list[str]:
    return __get_table_names(context)


@op(
    config_schema={
        "schema": Field(String, default_value="dbo"),
        "exclude": Field(
            Array(String),
            description="List of tables names to exclude from processing. Values will be parsed as regular expressions.",
            default_value=[],
            is_required=False,
        ),
    },
    out=DynamicOut(String, description="The names of tables in the specifed schema"),
    required_resource_keys={"sql_server"},
)
def get_table_names_dynamic(context: OpExecutionContext) -> list[str]:
    schema = context.op_config["schema"]

    for name in __get_table_names(context):
        yield DynamicOutput(
            value=name,
            # create a mapping key from the file name
            mapping_key=f"{schema}.{name}".replace(".", "_").replace("-", "_").lower(),
        )
