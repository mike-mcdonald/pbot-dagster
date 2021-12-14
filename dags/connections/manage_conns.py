import json

from conn_manager.conn import create_conn_table, upsert_conn, get_conn

from dagster import pipeline, solid, repository, ModeDefinition, fs_io_manager, Field
from dagster.builtins import Int


@solid
def create_table():
    create_conn_table()


@solid(
    config_schema={
        "name": Field(str),
        "host": Field(str, default_value=""),
        "schema": Field(str, default_value=""),
        "login": Field(str),
        "password": Field(str),
        "port": Field(Int, default_value=0),
        "extra": Field(dict, default_value={}),
    }
)
def upsert_connection(context, order):
    name = str(context.solid_config["name"]).strip()
    host = context.solid_config["host"]
    schema = context.solid_config["schema"]
    login = str(context.solid_config["login"]).strip()
    password = context.solid_config["password"]
    port = context.solid_config["port"]
    extra = context.solid_config["extra"]

    conn_info = {
        "name": name,
        "host": host,
        "schema": schema,
        "login": login,
        "password": password,
        "port": port,
        "extra": json.dumps(extra),
    }

    upsert_conn(conn_info)


@solid(
    config_schema={
        "conn_name": Field(str),
    }
)
def show_conn(context, order):
    context.log.info(get_conn(context.solid_config["conn_name"]))


@pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": fs_io_manager})])
def conn_pipeline():
    a = create_table()
    b = upsert_connection(order=a)
    show_conn(order=b)


@repository
def conn_repo():
    return [conn_pipeline]
