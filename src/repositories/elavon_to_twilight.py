import os

from datetime import datetime
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    fs_io_manager,
    job,
    op,
    repository,
    sensor,
)

from ops.append_columns import append_columns_to_csv
from ops.azure import upload_file
from ops.fs import remove_file

from resources import adls2_resource

DIRECTORY = "//pbotdm2/pudl/parking/elavon"


@op(config_schema={"filename": str})
def pass_file(context):
    filename = context.op_config["filename"]
    context.log.info(f"Processing {filename}...")
    return filename


@job(
    resource_defs={
        "adls2_resource": adls2_resource,
        "io_manager": fs_io_manager,
    }
)
def elavon_to_twilight():
    remove_file(upload_file(append_columns_to_csv(pass_file())))


@sensor(job=elavon_to_twilight, minimum_interval_seconds=(60 * 60))
def elavon_sensor(context: SensorEvaluationContext):
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime
    for filename in os.listdir(DIRECTORY):
        filepath = os.path.join(DIRECTORY, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime <= last_mtime:
                continue

            run_key = f"{filename}:{str(file_mtime)}"
            run_config = {
                "resources": {
                    "adls2_resource": {
                        "config": {
                            "azure_data_lake_gen2_conn_id": "azure_data_lake_gen2",
                        }
                    },
                },
                "ops": {
                    "pass_file": {"config": {"filename": filepath}},
                    "append_columns_to_csv": {
                        "config": {"map": {"seen": str(datetime.now())}}
                    },
                    "upload_file": {
                        "config": {
                            "container": "twilight",
                            "remote_path": "dagster/${pipeline_name}/${stem}${suffix}",
                        }
                    },
                },
            }
            yield RunRequest(run_key=run_key, run_config=run_config)
            max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))


@repository
def elavon_repo():
    return [elavon_to_twilight, elavon_sensor]
