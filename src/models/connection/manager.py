import logging

from typing import List

from models.connection.model import Connection
from models.base import session

log = logging.getLogger(__name__)


def add_connection(conn: Connection):
    session.add(conn)
    session.commit()
    return conn


def update_connection(conn_id: str, **kwargs):
    existing = get_connection(conn_id)

    for key, value in kwargs.items():
        existing.__setattr__(key, value)

    session.commit()
    return existing


def get_all_connection() -> List[Connection]:
    """
    Get connection by conn_id.

    :param conn_id: connection id
    :return: connection
    """
    conn = session.query(Connection)
    if conn:
        return conn


def get_connection(conn_id: str) -> Connection:
    """
    Get connection by conn_id.

    :param conn_id: connection id
    :return: connection
    """
    try:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if conn:
            return conn
    except Exception:  # pylint: disable=broad-except
        log.exception(f"Unable to retrieve connection `{conn_id}`")

    raise ValueError(f"The conn_id `{conn_id}` isn't defined")
