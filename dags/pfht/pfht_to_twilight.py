from datetime import datetime, time
from conn_manager.conn import get_conn
from pathlib import Path

from dagster import pipeline, solid, resource, daily_schedule, repository, ModeDefinition, Field, local_file_manager
from dagster.config import config_schema
# pip install dagster-azure, azure-core
from dagster_azure.adls2 import adls2_resource, ADLS2FileManager


@solid(
    config_schema={"local_path": Field(str),
                   "remote_path": Field(str),
                   "azure_container": Field(str),
                   },
    required_resource_keys={"adls2"}
)
def upload_file(context):
    adls_client = context.resources.adls2.adls2_client
    local_path = Path(str(context.solid_config["local_path"]))
    path_to = context.solid_config["remote_path"]
    container = context.solid_config["azure_container"]

    files: list(Path) = []

    if local_path.is_dir():
        files = [x for x in local_path.iterdir() if x.is_file()]
    else:
        files = [local_path]

    file_manager = ADLS2FileManager(
        adls2_client=adls_client, file_system=container, prefix=path_to)

    for file in files:
        with file.open("rb") as f:
            file_manager.write(f, ext=str(file.suffix)[1:])


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={'adls2': adls2_resource}
        )
    ]
)
def pfht_to_twilight():
    upload_file()


@daily_schedule(
    pipeline_name="pfht_to_twilight",
    start_date=datetime(2021, 5, 10),
    execution_time=time(hour=12, minute=40),
    execution_timezone="US/Pacific",
)
def pfht_schedule(date):
    conn = get_conn("azure_data_lake_gen2")
    return {
        "resources": {
            "adls2": {
                "config": {
                    "storage_account": str(conn.get("host")),
                    "credential": {
                        "key": str(conn.get("password")),
                    }
                }
            }
        },
        "solids": {
            "upload_file": {
                "config": {
                    "azure_container": "twilight",
                    "local_path": "//pbotfile1/PcCommon/abdullah/test_dagster/",
                    "remote_path": "test/pfht"
                }
            }
        }
    }


@repository
def pfht_repo():
    return [pfht_to_twilight, pfht_schedule]
