import os

from dagster import schedule, RunRequest, ScheduleEvaluationContext, SkipReason

from models.connection import get_connection
from repositories.enforcement.unregistered_vehicles import process_politess_exports


@schedule(cron_schedule="0 * * * *", job=process_politess_exports)
def parking_citations_to_featureclass(context: ScheduleEvaluationContext):
    execution_stamp = context.scheduled_execution_time.strftime("%Y%m%dT%H%M%S")
    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "Citations_",
        execution_stamp
    )

    remote_path = "Vertical_Apps/Citations"
    files = context.resources.ssh_client.list(remote_path)

    if len(files) == 0:
        return SkipReason("No files found in import directory")

    return RunRequest(
        run_key=context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
        run_config={
            "resources": {
                "geodatabase": {"config": {"conn_id": "mssql_ags_pbot"}},
                "ssh_client": {"config": {"conn_id": "sftp_citation_test"}},
            },
            "get_citations": {
                "config": {"remote_path": "Vertical_Apps/Citations"}
                },
            "download_citations": {
                "config": {
                    "remote_path": "Vertical_Apps/Citations",
                    "local_path": store_path
                    }
                },
            "ops": {
                "list_dir": {"config": {"path": store_path}},
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
                "append_features": {
                    "config": {
                        "feature_class": "AGS_MAINT_PBOT.PBOT_ADMIN.ParkingCitation_Test"
                    }
                },
            },
        },
    )
