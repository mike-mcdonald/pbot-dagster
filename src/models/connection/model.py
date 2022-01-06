import json
import logging
import os
import warnings
from json import JSONDecodeError
from typing import Dict, Optional, Union

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym

from models.base import Base
from models.connection.crypto import encrypt, decrypt


log = logging.getLogger(__name__)


class Connection(Base):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.

    .. seealso::
        For more information on how to use this class, see: :doc:`/howto/connection`

    :param conn_id: The connection ID.
    :type conn_id: str
    :param conn_type: The connection type.
    :type conn_type: str
    :param description: The connection description.
    :type description: str
    :param host: The host.
    :type host: str
    :param login: The login.
    :type login: str
    :param password: The password.
    :type password: str
    :param schema: The schema.
    :type schema: str
    :param port: The port number.
    :type port: int
    :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
        encoded object.
    :type extra: str
    :param uri: URI address describing connection parameters.
    :type uri: str
    """

    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(250), unique=True, nullable=False)
    description = Column(Text())
    host = Column(String(500))
    database = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    _password = Column("password", String(5000))
    port = Column(Integer())
    _extra = Column("extra", Text())

    def __init__(
        self,
        conn_id: Optional[str] = None,
        conn_type: Optional[str] = None,
        description: Optional[str] = None,
        host: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        extra: Optional[Union[str, dict]] = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.description = description
        if extra and not isinstance(extra, str):
            extra = json.dumps(extra)

        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra

    def get_password(self) -> Optional[str]:
        """Return encrypted password."""
        key = os.environ.get("DAGSTER_AES_KEY").strip()
        return decrypt(key, self._password)

    def set_password(self, value: Optional[str]):
        """Encrypt password and set in object attribute."""
        if value:
            key = os.environ.get("DAGSTER_AES_KEY").strip()
            self._password = encrypt(key, value)

    @declared_attr
    def password(cls):
        """Password. The value is decrypted/encrypted when reading/setting the value."""
        return synonym(
            "_password", descriptor=property(cls.get_password, cls.set_password)
        )

    def get_extra(self) -> Dict:
        """Return encrypted extra-data."""
        key = os.environ.get("DAGSTER_AES_KEY").strip()
        return decrypt(key, self._extra)

    def set_extra(self, value: str):
        """Encrypt extra-data and save in object attribute to object."""
        if value:
            key = os.environ.get("DAGSTER_AES_KEY").strip()
            self._extra = encrypt(key, value)

    @declared_attr
    def extra(cls):
        """Extra data. The value is decrypted/encrypted when reading/setting the value."""
        return synonym("_extra", descriptor=property(cls.get_extra, cls.set_extra))

    def __repr__(self):
        return self.conn_id

    @property
    def extra_dejson(self) -> Dict:
        """Returns the extra property by deserializing json."""
        obj = {}
        if self.extra:
            try:
                obj = json.loads(self.extra)

            except JSONDecodeError:
                self.log.exception(
                    "Failed parsing the json for conn_id %s", self.conn_id
                )

        return obj
