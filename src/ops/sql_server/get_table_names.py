import re

from typing import (
    List as PyList,
)

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


def __get_table_names(context: OpExecutionContext) -> PyList[str]:
    conn: MSSqlServerResource = context.resources.sql_server
    schema = context.op_config["schema"]
    exclusions = context.op_config["exclude"]
    inclusions = context.op_config["include"]

    context.log.info(f"🚀 Retrieving tables in '{schema}' schema...")

    tables = conn.get_tables(context, schema)

    context.log.info(f"👍 Retrieved tables {tables}")

    if len(inclusions) > 0:
        context.log.info(f"Including tables based on '{inclusions}'...")
        for inclusion in inclusions:
            inclusion = re.compile(inclusion, re.IGNORECASE)
            tables = [t for t in tables if inclusion.match(t)]

    if len(exclusions) > 0:
        context.log.info(f"Excluding some tables based on '{exclusions}'...")
        for exclusion in exclusions:
            exclusion = re.compile(exclusion, re.IGNORECASE)
            tables = [t for t in tables if not exclusion.match(t)]

    context.log.info(f"Tables being processed: {tables}")

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
        "include": Field(
            Array(String),
            description="List of tables names to include in processing. Values will be parsed as regular expressions.",
            default_value=[],
            is_required=False,
        ),
    },
    out=Out(List[String], description="The names of tables in the specifed schema"),
    required_resource_keys={"sql_server"},
)
def get_table_names(context: OpExecutionContext) -> PyList[str]:
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
        "include": Field(
            Array(String),
            description="List of table names to include in processing. Values will be parsed as regular expressions.",
            default_value=[],
            is_required=False,
        ),
    },
    out=DynamicOut(String, description="The names of tables in the specifed schema"),
    required_resource_keys={"sql_server"},
)
def get_table_names_dynamic(context: OpExecutionContext) -> PyList[str]:
    schema = context.op_config["schema"]

    for name in __get_table_names(context):
        yield DynamicOutput(
            value=name,
            # create a mapping key from the file name
            mapping_key=f"{schema}.{name}".replace(".", "_").replace("-", "_").lower(),
        )
