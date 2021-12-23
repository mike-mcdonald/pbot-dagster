import csv

from datetime import datetime, time
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

from dagster import (
    Field,
    InputDefinition,
    List,
    ModeDefinition,
    Nothing,
    OutputDefinition,
    fs_io_manager,
    pipeline,
    repository,
    solid,
    daily_schedule,
)
from dagster.core.definitions.decorators.schedule import daily_schedule
from dagster_resource.azure_resource.azure_data_lake_gen2 import azure_pudl_resource


@solid(
    config_schema={
        "local_path": Field(str),
        "host": Field(str),
        "schema": Field(str),
    },
    required_resource_keys={"azure_pudl_resource"},
)
def sql_to_csv(context) -> Nothing:
    local_path = Path(str(context.solid_config["local_path"]))
    server_and_db = str(context.solid_config["host"])
    schema = str(context.solid_config["schema"])
    engine = create_engine(
        f"mssql+pyodbc://{server_and_db}?Trusted_Connection=yes;driver=ODBC+Driver+17+for+SQL+Server")
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    batch_size = 1000

    if len(tables) > 0:
        for table in tables:
            context.log.info(f"started processing: {table}")
            file_name = local_path.joinpath(f"{table}.csv")
            cursor = engine.execute(
                text(f"select * from [{schema}].[{table}]"))
            columns = cursor.keys()
            with file_name.open("w", newline="") as f:
                writer = csv.writer(
                    f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
                )
                writer.writerow([x for x in columns])
                rows = cursor.fetchmany(batch_size)
                while len(rows) > 0:
                    writer.writerows(rows)
                    rows = cursor.fetchmany(batch_size)


@solid(
    config_schema={
        "local_path": Field(str),
        "remote_path": Field(str),
        "azure_container": Field(str),
        "execution_date": Field(str),
    },
    required_resource_keys={"azure_pudl_resource"},
    output_defs=[OutputDefinition(dagster_type=List)],
    input_defs=[InputDefinition("start", Nothing)],
)
def upload_file(context):
    client = context.resources.azure_pudl_resource
    local_path = str(context.solid_config["local_path"])
    path = Path(local_path)
    remote_path = context.solid_config["remote_path"]
    container = context.solid_config["azure_container"]
    execution_date = context.solid_config["execution_date"]

    files: List(Path) = []

    if path.is_dir():
        files = [x for x in path.iterdir() if x.is_file()]

        if len(files) > 0:
            for file in files:
                try:
                    client.upload_file(
                        file_system=container, local_path=str(file), remote_path=f"{remote_path}/{file.stem}/{execution_date}.{str(file.suffix)[1:]}")
                except Exception as err:
                    context.log.error(
                        f"Failed to write {local_path} to {remote_path}.")
                    raise err

        context.log.info(f"Number of files uploaded: {str(len(files))}")
    return files


@solid(input_defs=[InputDefinition("files", dagster_type=List)])
def cleanup_files(context, files):

    if len(files) > 0:
        for file in files:
            file.unlink(missing_ok=True)
        context.log.info(f"Number of files deleted: {str(len(files))}")
    else:
        context.log.info("No files found to delete.")


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"azure_pudl_resource": azure_pudl_resource,
                           "io_manager": fs_io_manager}
        )
    ]
)
def tsup_to_twilight():
    files = upload_file(sql_to_csv())
    cleanup_files(files)


@daily_schedule(
    pipeline_name="tsup_to_twilight",
    start_date=datetime(2021, 12, 21),
    execution_time=time(hour=6, minute=1),
    execution_timezone="US/Pacific",
)
def tsup_schedule(context):
    execution_date = context.scheduled_execution_time.strftime("%m-%d-%Y")
    return {
        "resources": {
            "azure_pudl_resource": {
                "config": {
                    "azure_data_lake_gen2_conn_id": "azure_data_lake_gen2",
                }
            }
        },
        "solids": {
            "sql_to_csv": {
                "config": {
                    "local_path": "//pbotdm1/pudl/tsup",
                    "host": "PBOTRPT1/TrackITExport",
                    "schema": "TSUP",
                }
            },
            "upload_file": {
                "config": {
                    "azure_container": "twilight",
                    "local_path": "//pbotdm1/pudl/tsup",
                    "remote_path": "tsup",
                    "execution_date": execution_date,
                }
            }
        },

    }


@repository
def tsup_repo():
    return [tsup_to_twilight, tsup_schedule]
