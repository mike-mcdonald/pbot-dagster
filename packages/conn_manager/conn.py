import os
import base64
import json

from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Util.Padding import pad, unpad

from typing import Any, Dict
from dagster.builtins import Int

from sqlalchemy import create_engine, Table, Column, String, MetaData, Integer
from sqlalchemy.sql import text
from sqlalchemy_utils import database_exists


def create_conn_table():
    url = f"""postgresql+psycopg2://{os.environ['DAGSTER_POSTGRES_USER']}:{os.environ['DAGSTER_POSTGRES_PASSWORD']}@{os.environ['DAGSTER_POSTGRES_HOST']}:{os.environ['DAGSTER_POSTGRES_PORT']}/{os.environ['DAGSTER_POSTGRES_DB']}"""
    engine = create_engine(url)
    if database_exists(engine.url):
        meta = MetaData()
        conn_table = Table('custom_conn', meta,
                           Column('name', String),
                           Column('host', String),
                           Column('schema', String),
                           Column('login', String),
                           Column('password', String),
                           Column('port', Integer),
                           Column('extra', String),
                           )
        conn_table.create(engine, checkfirst=True)


def _encrypt(key, data):
    data = pad(data.encode("utf8"), AES.block_size)
    key = key.encode("utf8")
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return (base64.b64encode(iv + cipher.encrypt(data))).decode("utf8")


def _decrypt(key, encoded):
    key = key.encode("utf8")
    encoded = base64.b64decode(encoded)
    iv = encoded[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return (unpad(cipher.decrypt(encoded[16:]), AES.block_size)).decode("utf8")


def upsert_conn(conn_info: Dict[str, Any]):
    url = f"postgresql+psycopg2://{os.environ['DAGSTER_POSTGRES_USER']}:{os.environ['DAGSTER_POSTGRES_PASSWORD']}@{os.environ['DAGSTER_POSTGRES_HOST']}:{os.environ['DAGSTER_POSTGRES_PORT']}/{os.environ['DAGSTER_POSTGRES_DB']}"
    engine = create_engine(url)
    conn = engine.connect()

    cols = conn_info.keys()
    if "name" not in cols:
        raise ValueError("conn_info must contain a connection name.")

    name = conn_info["name"].strip()
    host = conn_info['host'].strip() if 'host' in cols else ""
    schema = conn_info['schema'].strip() if 'schema' in cols else ""
    login = conn_info['login'].strip() if 'login' in cols else ""
    password = conn_info['password'].strip() if 'password' in cols else ""
    port = conn_info['port'] if 'port' in cols else 0
    extra = conn_info['extra'].strip() if 'extra' in cols else ""

    key = os.environ.get("DAGSTER_AES_KEY").strip()
    if login:
        login = _encrypt(key, login)
    if password:
        password = _encrypt(key, password)
    if extra:
        extra = _encrypt(key, extra)

    query = text(
        f"select * from custom_conn where name = '{name}'")
    result = conn.execute(query).fetchall()

    if len(result) > 0:
        query = text(f"""update custom_conn
                    set host = '{host}',
                    schema = '{schema}',
                    login = '{login}',
                    password = '{password}',
                    port = {port},
                    extra = '{extra}'
                    where name='{name}'""")
        conn.execute(query)
    else:
        query = text(f""" insert into custom_conn
                        (name, host, schema, login, password, port, extra)
                    values
                        ('{name}','{host}','{schema}','{login}','{password}',{port},'{extra}')""")
        conn.execute(query)
    conn.close()
    engine.dispose()


def get_conn(conn_name: str):
    url = f"postgresql+psycopg2://{os.environ['DAGSTER_POSTGRES_USER']}:{os.environ['DAGSTER_POSTGRES_PASSWORD']}@{os.environ['DAGSTER_POSTGRES_HOST']}:{os.environ['DAGSTER_POSTGRES_PORT']}/{os.environ['DAGSTER_POSTGRES_DB']}"
    engine = create_engine(url)
    conn = engine.connect()

    key = os.environ.get("DAGSTER_AES_KEY").strip()

    query = text(
        f"select * from custom_conn where name = '{conn_name.strip()}'")
    result = conn.execute(query).fetchone()
    result = result._asdict()
    if result:
        if result.get("login"):
            result["login"] = _decrypt(key, result['login'])
        if result.get("password"):
            result["password"] = _decrypt(key, result['password'])
        if result.get("extra"):
            result["extra"] = _decrypt(key, result['extra'])
            result["extra"] = json.loads(result["extra"])
        return result
    else:
        raise ValueError(f"Connection not found, Name={conn_name}")
