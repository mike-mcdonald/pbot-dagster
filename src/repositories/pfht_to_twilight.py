from datetime import datetime, timezone
from dagster import (
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SkipReason,
    fs_io_manager,
    job,
    repository,
    schedule,
)

from models.connection.manager import get_connection
from ops.azure import upload_file
from ops.fs import list_dir_dynamic, remove_files

from resources.azure_data_lake_gen2 import adls2_resource


@job(
    resource_defs={
        "adls2_resource": adls2_resource,
        "io_manager": fs_io_manager,
    }
)
def pfht_to_twilight():
    files = list_dir_dynamic().map(upload_file)
    remove_files(files.collect())

@schedule(job=pfht_to_twilight,cron_schedule="*/15 * * * *",execution_timezone="US/Pacific",)
def pfht_schedule(context: SensorEvaluationContext):

    execution_date = context.scheduled_execution_time.strftime("%Y%m%dT%H%M%S")

    from pathlib import Path

    fs = get_connection("fs_pfht_uploads")

    path = Path(fs.host)
    files = list(path.rglob("*.csv"))

    if len(files):
        return SkipReason("No files found in pfht import directory")


    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "adls2_resource": {
                    "config": {
                        "conn_id": "azure_data_lake_gen2",
                    }
                }
            },
            "ops": {
                "list_dir_dynamic": {"config": {"path": fs.host, "recursive": True}},
                "upload_file": {
                    "config": {
                        "base_dir": fs.host,
                        "container": "twilight",
                        "remote_path": "dagster/pfht_to_twilight/${parent}/${execution_date}/${name}",
                        "substitutions": {"execution_date": execution_date},
                    }
                },
            },
        },
    )

@repository
def pfht_repo():
    return [pfht_to_twilight, pfht_schedule]
