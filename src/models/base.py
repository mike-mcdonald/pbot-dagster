import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy_utils import database_exists

Base = declarative_base()

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['DAGSTER_POSTGRES_USER']}:{os.environ['DAGSTER_POSTGRES_PASSWORD']}@{os.environ['DAGSTER_POSTGRES_HOST']}:{os.environ['DAGSTER_POSTGRES_PORT']}/{os.environ['DAGSTER_POSTGRES_DB']}"
)
session = sessionmaker(bind=engine)()


def create_tables():
    if database_exists(engine.url):
        Base.metadata.create_all(engine)
