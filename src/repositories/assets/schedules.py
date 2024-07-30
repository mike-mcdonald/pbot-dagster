import os

from dagster import RunConfig, RunRequest, ScheduleEvaluationContext, schedule

from repositories.assets.jobs import (
    sign_library_to_assets,
    synchronize_bridge_fields,
    update_bridge_evaluations,
)


@schedule(
    job=synchronize_bridge_fields,
    cron_schedule="0 0 * * *",
    execution_timezone="US/Pacific",
)
def bridge_daily_schedule(context: ScheduleEvaluationContext):
    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "synchronize_bridge_fields",
        context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
    )

    return RunRequest(
        run_config=RunConfig(
            ops={
                "extract_pbot": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge\extract.py",
                        "conn_id": "mssql_server_assets",
                        "feature_class": "BRIDGE",
                        "output": os.path.join(store_path, "extract_pbot.json"),
                    }
                },
                "extract_public": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge\extract.py",
                        "conn_id": "mssql_ags_pbot",
                        "feature_class": "BRIDGE",
                        "output": os.path.join(store_path, "extract_public.json"),
                    }
                },
                "create_cgis_inserts": {
                    "config": {
                        "index_field": "AssetID",
                        "exclude_fields": [
                            "GlobalID",
                            "ClosureStatus",
                            "created_date",
                            "created_user",
                            "last_edited_date",
                            "last_edited_user",
                        ],
                        "destination": os.path.join(
                            store_path, "create_cgis_inserts.json"
                        ),
                    }
                },
                "create_cgis_updates": {
                    "config": {
                        "index_field": "AssetID",
                        "exclude_fields": [
                            "GlobalID",
                            "ClosureStatus",
                            "created_date",
                            "created_user",
                            "last_edited_date",
                            "last_edited_user",
                        ],
                        "destination": os.path.join(
                            store_path, "create_cgis_updates.json"
                        ),
                    }
                },
                "create_pbot_updates": {
                    "config": {
                        "index_field": "AssetID",
                        "retain_fields": [
                            "ClosureStatus",
                        ],
                        "destination": os.path.join(
                            store_path, "create_pbot_updates.json"
                        ),
                    }
                },
                "insert_cgis": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge\insert.py",
                        "conn_id": "mssql_ags_pbot",
                        "feature_class": "BRIDGE",
                    }
                },
                "update_cgis": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge\update.py",
                        "conn_id": "mssql_ags_pbot",
                        "feature_class": "BRIDGE",
                    }
                },
                "update_pbot": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge\update.py",
                        "conn_id": "mssql_server_assets",
                        "feature_class": "BRIDGE",
                    }
                },
            },
            resources={},
        )
    )


@schedule(
    job=update_bridge_evaluations,
    cron_schedule="*/10 * * * *",
    execution_timezone="US/Pacific",
)
def bridge_eval_updates(context: ScheduleEvaluationContext):
    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "update_bridge_evaluations",
        context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
    )

    return RunRequest(
        run_config=RunConfig(
            ops={
                "extract_asset": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge_eval\extract.py",
                        "conn_id": "mssql_ags_pbot",
                        "feature_class": "BRIDGEPOSTEARTHQUAKEEVALUATION",
                        "output": os.path.join(store_path, "extract_evals.json"),
                    }
                },
                "determine_eval_updates": {
                    "config": {
                        "destination": os.path.join(
                            store_path, "create_eval_updates.json"
                        ),
                    }
                },
                "update_asset": {
                    "config": {
                        "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                        "script_path": r"bridge_eval\update.py",
                        "conn_id": "mssql_ags_pbot",
                        "feature_class": "BRIDGE",
                    }
                },
            },
            resources={},
        )
    )


@schedule(
    job=sign_library_to_assets,
    cron_schedule="0 20 * * *",
    execution_timezone="US/Pacific",
)
def sign_library_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time.strftime("%Y%m%d")

    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "sql_server": {"config": {"conn_id": "mssql_server_assets"}},
            },
            "ops": {
                "signs_to_file": {
                    "config": {
                        "path": "//pbotdm2/pudl/assets/signlib/${execution_date}.parquet",
                        "substitutions": {"execution_date": execution_date},
                    },
                },
                "truncate_table": {
                    "config": {
                        "schema": "PDOT",
                    },
                    "inputs": {"table": "SIGNLIB"},
                },
                "file_to_table": {"config": {"schema": "PDOT"}},
            },
        },
    )
