import os

from dagster import (
    RunConfig,
    schedule,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
)

from repositories.enforcement.unregistered_vehicles import process_politess_exports
from resources.ssh import SSHClientResource


@schedule(cron_schedule="0 * * * *", job=process_politess_exports)
def parking_citations_to_featureclass(context: ScheduleEvaluationContext):
    execution_stamp = context.scheduled_execution_time.strftime("%Y%m%dT%H%M%S")

    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "parking_citations_to_featureclass",
        execution_stamp,
    )

    sftp_conn_id = "sftp_parking_citations"
    path = "/"

    resource: SSHClientResource = SSHClientResource(conn_id=sftp_conn_id)

    files = resource.list(path)

    if len(files) == 0:
        return SkipReason("No files found in import directory")

    return RunRequest(
        run_key=context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
        run_config=RunConfig(
            resources={
                "geodatabase": {"config": {"conn_id": "mssql_ags_pbot"}},
                "sftp": {"config": {"conn_id": sftp_conn_id}},
            },
            ops={
                "append_features": {
                    "config": {
                        "feature_class": "AGS_MAINT_PBOT.PBOT_ADMIN.ParkingCitation"
                    }
                },
                "create_dataframe": {
                    "config": {
                        "output": os.path.join(
                            store_path,
                            "{}.parquet".format(
                                context.scheduled_execution_time.strftime(
                                    r"%Y%m%dT%H%M%S"
                                )
                            ),
                        )
                    }
                },
                "download_list": {
                    "config": {"path": os.path.join(store_path, "download_list")}
                },
                "list": {"config": {"path": path, "pattern": "csv"}},
            },
        ),
    )
