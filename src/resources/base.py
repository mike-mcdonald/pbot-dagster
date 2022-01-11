from models.connection import Connection, get_connection


class BaseResource:
    def get_connection(self, conn_id: str) -> Connection:
        """
        Returns connection from Connection table
        """
        return get_connection(conn_id)
