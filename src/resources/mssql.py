from contextlib import contextmanager
from textwrap import dedent
from typing import List

from dagster import OpExecutionContext, resource

import pyodbc

from resources.base import BaseResource


class MSSqlServerResource(BaseResource):
    def __init__(
        self,
        sql_server_conn_id="sql_server",
    ):
        self.sql_server_conn_id = sql_server_conn_id
        self.client = self.get_connection()

    def get_connection(self) -> pyodbc.Connection:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        connection = super().get_connection(self.sql_server_conn_id)

        connection_args = [
            f"SERVER={connection.host}",
            "DRIVER={ODBC Driver 17 for SQL Server}",
        ]

        if connection.schema:
            connection_args.append(f"DATABASE={connection.schema}")

        # If we have login/password in the connection object use that,
        # otherwise assume it is using Windows authentication
        connection_args.extend(
            [f"UID={connection.login}", f"PWD={connection.password}"]
        ) if connection.login is not None else connection_args.append(
            "Trusted_Connection=yes"
        )

        for key, value in connection.extra_dejson.items():
            connection_args.append(f"{str(key).upper()}={value}")

        return pyodbc.connect(";".join(connection_args), autocommit=True)

    def get_tables(
        self,
        context: OpExecutionContext,
        schema: str = None,
    ) -> List[str]:
        cursor = self.execute(
            context,
            f"""
        select table_name
        from information_schema.tables
        where table_type = 'BASE TABLE'
        and table_name <> 'sysdiagrams'
        {f"and table_schema = ?" if schema is not None else ""}
        """,
            schema,
        )

        rows = cursor.fetchall()

        return [x.table_name for x in rows]

    def execute(self, context: OpExecutionContext, sql: str, *params):
        context.log.debug(f"🚀 Executing: {dedent(sql)} with params: {params}")
        return self.client.execute(sql, *params)


@resource(config_schema={"mssql_server_conn_id": str})
@contextmanager
def mssql_resource(init_context):
    conn = MSSqlServerResource(
        sql_server_conn_id=init_context.resource_config["mssql_server_conn_id"]
    )
    try:
        yield conn
    finally:
        conn.client.close()
