import os

from dagster import schedule, RunRequest, ScheduleEvaluationContext, SkipReason

from models.connection import get_connection
from repositories.enforcement.unregistered_vehicles import process_politess_exports


@schedule(cron_schedule="*/15 * * * *", job=process_politess_exports)
def parking_citations_to_featureclass(context: ScheduleEvaluationContext):
    fs = get_connection("fs_politess_import")

    files = os.listdir(fs.host)

    if len(files) == 0:
        return SkipReason("No files found in import directory")

    return RunRequest(
        run_key=context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
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
                            "{}.parquet".format(
                                context.scheduled_execution_time.strftime(
                                    r"%Y%m%dT%H%M%S"
                                )
                            ),
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
