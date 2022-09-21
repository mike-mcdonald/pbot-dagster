from dagster import (
    RunRequest,
    ScheduleEvaluationContext,
    fs_io_manager,
    job,
    repository,
    schedule,
)

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


@schedule(
    job=pfht_to_twilight,
    cron_schedule="0 6 * * *",
    execution_timezone="US/Pacific",
)
def pfht_schedule(context: ScheduleEvaluationContext):
    DIRECTORY = "//pbotdm1/pudl/pfht"
    exectution_date = context.scheduled_execution_time.strftime("%Y%m%d")

    return RunRequest(
        run_key=exectution_date,
        run_config={
            "resources": {
                "adls2_resource": {
                    "config": {
                        "azure_data_lake_gen2_conn_id": "azure_data_lake_gen2",
                    }
                }
            },
            "ops": {
                "list_dir_dynamic": {"config": {"path": DIRECTORY, "recursive": True}},
                "upload_file": {
                    "config": {
                        "base_dir": DIRECTORY,
                        "container": "twilight",
                        "remote_path": "dagster/${pipeline_name}/${parent}/${execution_date}/${name}",
                        "substitutions": {"execution_date": exectution_date},
                    }
                },
            },
        },
    )


@repository
def pfht_repo():
    return [pfht_to_twilight, pfht_schedule]
