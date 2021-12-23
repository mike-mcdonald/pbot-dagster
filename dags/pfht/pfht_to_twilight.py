from datetime import datetime, time
from pathlib import Path

from dagster import (
    Field,
    InputDefinition,
    List,
    ModeDefinition,
    OutputDefinition,
    fs_io_manager,
    pipeline,
    repository,
    solid,
    weekly_schedule,
)
from dagster.core.definitions.decorators.schedule import weekly_schedule
from dagster_resource.azure_resource.azure_data_lake_gen2 import azure_pudl_resource


@solid(
    config_schema={
        "local_path": Field(str),
        "remote_path": Field(str),
        "azure_container": Field(str),
        "execution_date": Field(str),
    },
    required_resource_keys={"azure_pudl_resource"},
    output_defs=[OutputDefinition(dagster_type=List)],
)
def upload_file(context):
    client = context.resources.azure_pudl_resource
    local_path = Path(str(context.solid_config["local_path"]))
    remote_path = context.solid_config["remote_path"]
    container = context.solid_config["azure_container"]
    execution_date = context.solid_config["execution_date"]

    dirs: List(Path) = []
    _files: List(Path) = []

    if local_path.is_dir():
        dirs = [x for x in local_path.iterdir() if x.is_dir()]
        if len(dirs) > 0:
            count = 0
            for dir in dirs:
                files = [x for x in dir.iterdir() if x.is_file()]
                if len(files) > 0:
                    for file in files:
                        try:
                            client.upload_file(
                                file_system=container,
                                local_path=str(file),
                                remote_path=f"{str(remote_path)}/{dir.name}/{execution_date}/{file.stem}.{str(file.suffix)[1:]}")
                        except Exception as err:
                            context.log.error(
                                f"Failed to write {local_path} to {str(remote_path)}/{dir.name}/{execution_date}/{file.stem}.{str(file.suffix)[1:]}.")
                            raise err
                        count += 1
                        _files.append(file)
            context.log.info(f"Number of files uploaded: {str(count)}")
    return _files


@solid(input_defs=[InputDefinition("files", dagster_type=List)])
def cleanup_files(context, files):

    if len(files) > 0:
        for file in files:
            file.unlink(missing_ok=True)
        context.log.info(f"Number of files deleted: {str(len(files))}")
    else:
        context.log.info("No files found to delete.")


# fs_io_manager is for persistent I/O that enables re-execution of tasks.
@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"azure_pudl_resource": azure_pudl_resource,
                           "io_manager": fs_io_manager}
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
def pfht_schedule(context):
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
            "upload_file": {
                "config": {
                    "azure_container": "twilight",
                    "local_path": "//pbotdm1/pudl/pfht",
                    "remote_path": "pfht",
                    "execution_date": execution_date,
                }
            }
        },
    }


@repository
def pfht_repo():
    return [pfht_to_twilight, pfht_schedule]
