import os

from dagster import (
    RunConfig,
    RunRequest,
    schedule,
    ScheduleEvaluationContext,
    SkipReason,
)

from resources.ssh import SSHClientResource
from .job import analyze_driver_records


@schedule(
    job=analyze_driver_records,
    cron_schedule="0 * * * *",
    execution_timezone="US/Pacific",
)
def analyze_driver_records(context: ScheduleEvaluationContext):
    import stat

    SFTP_CONN_ID = "sftp_driver_records"
    LYFT_FOLDER = "Lyft_Background_Docs"
    UBER_FOLDER = "Uber_Background_Docs"

    sftp: SSHClientResource = SSHClientResource(SFTP_CONN_ID)

    files = []
    for path in [LYFT_FOLDER, UBER_FOLDER]:
        files.extend([f for f in sftp.list(path) if not stat.S_ISDIR(f.st_mode)])

    if len(files) == 0:
        return SkipReason("No files in any shares")

    execution_stamp = context.scheduled_execution_time.strftime("%Y%m%dT%H%M%S")

    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "analyze_driver_records",
        execution_stamp,
    )

    return RunRequest(
        run_key=context.scheduled_execution_time.isoformat(),
        run_config=RunConfig(
            ops={
                "analyze_lyft_bgc": {"config": {"method": "status"}},
                "analyze_lyft_mvr": {"config": {"method": "violations"}},
                "analyze_uber_bgc": {
                    "config": {
                        "method": "status",
                    }
                },
                "analyze_uber_mvr": {"config": {"method": "status"}},
                "cleanup_dir": {
                    "config": {
                        "path": store_path,
                        "recursive": True,
                    }
                },
                "create_rename_dataframe": {"config": {"paths": ["ValidationFolder"]}},
                "download_lyft_bgc": {
                    "config": {
                        "filter": {
                            "company": "Lyft",
                            "doc_type": "BGC",
                        }
                    }
                },
                "download_lyft_mvr": {
                    "config": {"filter": {"company": "Lyft", "doc_type": "DMV"}}
                },
                "download_uber_bgc": {
                    "config": {"filter": {"company": "Uber", "doc_type": "BGC"}}
                },
                "download_uber_mvr": {
                    "config": {"filter": {"company": "Uber", "doc_type": "MVR"}}
                },
                "filter_lyft_results": {"config": {"filters": {"company": "Lyft"}}},
                "filter_uber_results": {"config": {"filters": {"company": "Uber"}}},
                "get_lyft_files": {
                    "config": {
                        "company": "Lyft",
                        "local_path": os.path.join(store_path, "data", "lyft"),
                        "remote_path": LYFT_FOLDER,
                    }
                },
                "get_uber_files": {
                    "config": {
                        "company": "Uber",
                        "local_path": os.path.join(store_path, "data", "uber"),
                        "remote_path": UBER_FOLDER,
                    }
                },
                "list_lyft_csv": {"config": {"path": LYFT_FOLDER, "pattern": "csv"}},
                "list_uber_csv": {"config": {"path": UBER_FOLDER, "pattern": "csv"}},
                "move_lyft_csv": {
                    "config": {"path": rf"{LYFT_FOLDER}\ValidationFolder"}
                },
                "move_uber_csv": {
                    "config": {"path": rf"{UBER_FOLDER}\ValidationFolder"}
                },
                "put_lyft_results": {
                    "config": {"path": rf"{LYFT_FOLDER}\ResultFolder"}
                },
                "put_uber_results": {
                    "config": {"path": rf"{UBER_FOLDER}\ResultFolder"}
                },
                "write_lyft_results": {
                    "config": {
                        "path": os.path.join(
                            store_path,
                            f"lyft_analysis_{execution_stamp}.csv",
                        )
                    }
                },
                "write_uber_results": {
                    "config": {
                        "path": os.path.join(
                            store_path,
                            f"uber_analysis_{execution_stamp}.csv",
                        )
                    }
                },
            },
            resources={"sftp": {"config": {"conn_id": SFTP_CONN_ID}}},
        ),
    )
