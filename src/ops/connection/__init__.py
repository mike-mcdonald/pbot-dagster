from dagster import (
    Dict,
    DynamicOut,
    DynamicOutput,
    Field,
    Int,
    Noneable,
    OpExecutionContext,
    Out,
    op,
)

from models import (
    Connection,
    add_connection as add_model,
    update_connection as update_model,
    get_all_connection,
)


def __to_dict(conn: Connection):
    return {
        "id": conn.id,
        "conn_id": conn.conn_id,
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn._password,
        "port": conn.port,
        "extra": conn._extra,
    }


@op(out=DynamicOut(Dict))
def list_connections():
    for c in get_all_connection():
        yield DynamicOutput(
            value=__to_dict(c),
            mapping_key=c.conn_id,
        )


@op(
    config_schema={
        "conn_id": Field(str),
        "host": Field(Noneable(str), is_required=False),
        "schema": Field(Noneable(str), is_required=False),
        "login": Field(Noneable(str), is_required=False),
        "password": Field(Noneable(str), is_required=False),
        "port": Field(Noneable(Int), is_required=False),
        "extra": Field(Noneable(dict), is_required=False),
    },
    out=Out(Dict),
)
def add_connection(context: OpExecutionContext):
    conn: Connection = Connection(
        conn_id=context.op_config["conn_id"],
        host=context.op_config["host"],
        schema=context.op_config["schema"],
        login=context.op_config["login"],
        password=context.op_config["password"],
        port=context.op_config["port"],
        extra=context.op_config["extra"],
    )

    conn = add_model(conn)

    return __to_dict(conn)


@op(
    config_schema={
        "conn_id": Field(str),
        "host": Field(Noneable(str), is_required=False),
        "schema": Field(Noneable(str), is_required=False),
        "login": Field(Noneable(str), is_required=False),
        "password": Field(Noneable(str), is_required=False),
        "port": Field(Noneable(Int), is_required=False),
        "extra": Field(Noneable(dict), is_required=False),
    },
    out=Out(Dict),
)
def update_connection(context: OpExecutionContext):
    kwargs = {}

    for prop in {
        "host",
        "schema",
        "login",
        "password",
        "port",
        "extra",
    }:
        if prop in context.op_config:
            kwargs[prop] = context.op_config[prop]

    conn = update_model(context.op_config["conn_id"], **kwargs)

    return __to_dict(conn)
