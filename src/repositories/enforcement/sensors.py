import os

from datetime import datetime

from dagster import sensor, RunRequest, SensorEvaluationContext, SkipReason

from models.connection import get_connection
from repositories.enforcement.unregistered_vehicles import process_politess_exports


@sensor(job=process_politess_exports, minimum_interval_seconds=(60 * 15))
def politess_sensor():
    fs = get_connection("fs_politess_import")

    files = os.listdir(fs.host)

    if len(files) == 0:
        return SkipReason("No files found in import directory")

    now = datetime.now()

    return RunRequest(
        run_key=now.strftime(r"%Y%m%dT%H%M%S"),
        run_config={
            "resources": {
                "geodatabase": {"config": {"conn_id": "mssql_ags_pbot"}},
            },
            "ops": {
                "list_dir": {"config": {"path": fs.host}},
                "create_dataframe": {
                    "config": {
                        "output": os.path.join(
                            fs.host,
                            "{}.parquet".format(now.strftime(r"%Y%m%dT%H%M%S")),
                        )
                    }
                },
                "append_features": {
                    "config": {
                        "feature_class": "AGS_MAINT_PBOT.PBOT_ADMIN.ParkingCitation"
                    }
                },
            },
        },
    )
