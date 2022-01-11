import json
from typing import Dict

from dagster import (
    OpExecutionContext,
    job,
    op,
    repository,
)

from ops.connection import add_connection, list_connections, update_connection


@op
def show_connection(context: OpExecutionContext, connection: Dict):
    context.log.info(json.dumps(connection, indent=2))


@job
def show_connections():
    list_connections().map(show_connection)


@job
def insert_connection():
    conn = add_connection()
    show_connection(conn)


@job
def change_connection():
    conn = update_connection()
    show_connection(conn)


@repository
def connections():
    return [change_connection, insert_connection, show_connections]
