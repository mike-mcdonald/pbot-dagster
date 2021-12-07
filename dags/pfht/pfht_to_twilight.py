from datetime import datetime, time
from conn_manager.conn import get_conn
from pathlib import Path

from dagster import pipeline, solid, weekly_schedule, repository, ModeDefinition, Field, fs_io_manager, OutputDefinition, InputDefinition, List
from dagster.config import config_schema
from dagster.core.definitions.decorators.schedule import weekly_schedule
# pip install dagster-azure, azure-core
from dagster_azure.adls2 import adls2_resource, ADLS2FileManager
from sqlalchemy.sql.expression import true


@solid(
    config_schema={"local_path": Field(str),
                   "remote_path": Field(str),
                   "azure_container": Field(str),
                   },
    required_resource_keys={"adls2"},
    output_defs=[OutputDefinition(dagster_type=List)]
)
def upload_file(context):
    adls_client = context.resources.adls2.adls2_client
    local_path = Path(str(context.solid_config["local_path"]))
    path_to = context.solid_config["remote_path"]
    container = context.solid_config["azure_container"]
    run_id = context.run_id

    dirs: List(Path) = []
    _files: List(Path) = []

    if local_path.is_dir():
        dirs = [x for x in local_path.iterdir() if x.is_dir()]
        if len(dirs) > 0:
            count = 0
            for dir in dirs:
                files = [x for x in dir.iterdir() if x.is_file()]
                file_manager = ADLS2FileManager(
                    adls2_client=adls_client, file_system=container, prefix=f"{path_to}/{dir.name}/{run_id}")
                if len(files) > 0:
                    count = count + len(files)
                    for file in files:
                        with file.open("rb") as f:
                            file_manager.write(f, ext=str(file.suffix)[1:])
                        _files.append(file)
                else:
                    context.log.info(
                        f"No files found to upload in: {str(dir)}")
            context.log.info(f"Number of files uploaded: {str(count)}")
    return _files


@solid(input_defs=[InputDefinition('files', dagster_type=List)])
def cleanup_files(context, files):

    if len(files) > 0:
        for file in files:
            file.unlink(missing_ok=true)
        context.log.info(f"Number of files deleted: {str(len(files))}")
    else:
        context.log.info("No files found to delete.")


# fs_io_manager is for persistent I/O that enables re-execution of tasks.
@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                'adls2': adls2_resource,
                'io_manager': fs_io_manager
            }
        )
    ]
)
def pfht_to_twilight():
    files = upload_file()
    cleanup_files(files=files)


@weekly_schedule(
    pipeline_name="pfht_to_twilight",
    start_date=datetime(2021, 12, 6),
    execution_day_of_week=5,
    execution_time=time(hour=19, minute=1),
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
                    "local_path": "//pbotdm1/pudl/pfht",
                    "remote_path": "pfht"
                }
            }
        }
    }


@repository
def pfht_repo():
    return [pfht_to_twilight, pfht_schedule]
