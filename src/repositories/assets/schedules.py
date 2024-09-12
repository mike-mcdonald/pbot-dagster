import os

from datetime import timedelta

from dagster import RunConfig, RunRequest, ScheduleEvaluationContext, schedule

from repositories.assets.jobs import (
    sign_library_to_assets,
    synchronize_bridge_fields,
    update_bridge_evaluations,
)


def create_bridge_eval_config(
    context: ScheduleEvaluationContext,
    environment: str,
    cgis_conn: str,
    evaluation_feature_class: str,
    bridge_feature_class: str,
):
    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "update_bridge_evaluations",
        environment,
        context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
    )

    return RunConfig(
        ops={
            "extract_asset": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge_eval\extract.py",
                    "conn_id": cgis_conn,
                    "feature_class": evaluation_feature_class,
                    "output": os.path.join(store_path, "extract_evals.json"),
                }
            },
            "determine_eval_updates": {
                "config": {
                    "datetime": (
                        context.scheduled_execution_time - timedelta(minutes=10)
                    ).isoformat(),
                    "destination": os.path.join(store_path, "create_eval_updates.json"),
                }
            },
            "update_asset": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge_eval\update.py",
                    "conn_id": cgis_conn,
                    "feature_class": bridge_feature_class,
                }
            },
        },
        resources={},
    )


def create_bridge_eval_schedule(
    name: str,
    environment: str,
    cgis_conn: str,
    evaluation_feature_class: str,
    bridge_feature_class: str,
):
    @schedule(
        "*/10 * * * *",
        name=name,
        job=update_bridge_evaluations,
        execution_timezone="US/Pacific",
    )
    def _inner_schedule(context: ScheduleEvaluationContext):
        return RunRequest(
            run_config=create_bridge_eval_config(
                context,
                environment,
                cgis_conn,
                evaluation_feature_class,
                bridge_feature_class,
            )
        )

    return _inner_schedule


prod_bridge_eval_schedule = create_bridge_eval_schedule(
    "prod_bridge_eval_schedule",
    "prod",
    "mssql_ags_pbot",
    "BRIDGEPOSTEARTHQUAKEEVALUATION",
    "BRIDGE",
)

test_bridge_eval_schedule = create_bridge_eval_schedule(
    "test_bridge_eval_schedule",
    "test",
    "mssql_ags_pbot",
    "BRIDGEPOSTEARTHQUAKEEVALUATION_TEST",
    "BRIDGE_TEST",
)


def create_bridge_sync_config(
    context: ScheduleEvaluationContext,
    environment: str,
    pbot_conn: str,
    pbot_feature_class: str,
    cgis_conn: str,
    cgis_feature_class: str,
):
    store_path = os.path.join(
        os.getenv("DAGSTER_DATA_BASEPATH"),
        "synchronize_bridge_fields",
        environment,
        context.scheduled_execution_time.strftime(r"%Y%m%dT%H%M%S"),
    )

    return RunConfig(
        ops={
            "extract_pbot": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge\extract.py",
                    "conn_id": pbot_conn,
                    "feature_class": pbot_feature_class,
                    "output": os.path.join(store_path, "extract_pbot.json"),
                }
            },
            "extract_public": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge\extract.py",
                    "conn_id": cgis_conn,
                    "feature_class": cgis_feature_class,
                    "output": os.path.join(store_path, "extract_public.json"),
                }
            },
            # "filter_cgis": {
            #     "config": {
            #         "datetime": (
            #             context.scheduled_execution_time - timedelta(days=1)
            #         ).isoformat(),
            #         "destination": os.path.join(store_path, "filter_cgis.json"),
            #         "field": "last_edited_date",
            #     }
            # },
            # "filter_pbot": {
            #     "config": {
            #         "datetime": (
            #             context.scheduled_execution_time - timedelta(days=1)
            #         ).isoformat(),
            #         "destination": os.path.join(store_path, "filter_pbot.json"),
            #         "field": "ModifiedOn",
            #     }
            # },
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
                    "destination": os.path.join(store_path, "create_cgis_inserts.json"),
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
                    "destination": os.path.join(store_path, "create_cgis_updates.json"),
                }
            },
            "create_pbot_updates": {
                "config": {
                    "index_field": "AssetID",
                    "retain_fields": [
                        "ClosureStatus",
                    ],
                    "destination": os.path.join(store_path, "create_pbot_updates.json"),
                }
            },
            "insert_cgis": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge\insert.py",
                    "conn_id": cgis_conn,
                    "feature_class": cgis_feature_class,
                }
            },
            "update_cgis": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge\update.py",
                    "conn_id": cgis_conn,
                    "feature_class": cgis_feature_class,
                }
            },
            "update_pbot": {
                "config": {
                    "python_exe": r"C:\Python27\ArcGIS10.7\python.exe",
                    "script_path": r"bridge\update.py",
                    "conn_id": pbot_conn,
                    "feature_class": pbot_feature_class,
                }
            },
        },
    )


def create_bridge_sync_schedule(
    name: str,
    environment: str,
    pbot_conn: str,
    pbot_feature_class: str,
    cgis_conn: str,
    cgis_feature_class: str,
):

    @schedule(
        "0 0 * * *",
        name=name,
        job=synchronize_bridge_fields,
        execution_timezone="US/Pacific",
    )
    def _inner_schedule(context: ScheduleEvaluationContext):
        return RunRequest(
            run_config=create_bridge_sync_config(
                context,
                environment,
                pbot_conn,
                pbot_feature_class,
                cgis_conn,
                cgis_feature_class,
            )
        )

    return _inner_schedule


prod_bridge_field_schedule = create_bridge_sync_schedule(
    "prod_bridge_field_schedule",
    "prod",
    "mssql_server_assets",
    "BRIDGE",
    "mssql_ags_pbot",
    "BRIDGE",
)

test_bridge_field_schedule = create_bridge_sync_schedule(
    "test_bridge_field_schedule",
    "test",
    "mssql_assets_test",
    "BRIDGE",
    "mssql_ags_pbot",
    "BRIDGE_TEST",
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
