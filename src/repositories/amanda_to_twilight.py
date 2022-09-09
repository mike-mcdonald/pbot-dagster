from datetime import datetime
from dagster import (
    ScheduleEvaluationContext,
    fs_io_manager,
    job,
    repository,
    schedule,
)

from ops.append_columns import append_columns_to_parquet
from ops.azure import upload_file
from ops.fs import remove_files
from ops.sql_server import get_table_names_dynamic, table_to_parquet

from resources import adls2_resource
from resources.mssql import mssql_resource


@job(
    resource_defs={
        "adls2_resource": adls2_resource,
        "sql_server": mssql_resource,
        "io_manager": fs_io_manager,
    }
)
def amanda_to_twilight():
    files = (
        get_table_names_dynamic()
        .map(table_to_parquet)
        .map(append_columns_to_parquet)
        .map(upload_file)
    )

    remove_files(files.collect())


@schedule(
    job=amanda_to_twilight,
    cron_schedule="0 1 * * *",
    execution_timezone="US/Pacific",
)
def amanda_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time.strftime("%Y%m%d")
    return {
        "resources": {
            "adls2_resource": {
                "config": {
                    "azure_data_lake_gen2_conn_id": "azure_data_lake_gen2",
                }
            },
            "sql_server": {"config": {"mssql_server_conn_id": "mssql_server_amanda"}},
        },
        "ops": {
            "get_table_names_dynamic": {
                "config": {"schema": "dbo", "include": ["PBOT_ROW_COORDINATION"]}
            },
            "table_to_parquet": {
                "config": {
                    "schema": "dbo",
                    "path": "//pbotdm1/pudl/amanda/${execution_date}/${table}.parquet",
                    "substitutions": {"execution_date": execution_date},
                }
            },
            "append_columns_to_parquet": {"map": {"seen": datetime.now()}},
            "upload_file": {
                "config": {
                    "container": "twilight",
                    "remote_path": "dagster/${pipeline_name}/${stem}/${execution_date}${suffix}",
                    "substitutions": {"execution_date": execution_date},
                }
            },
        },
    }


@repository
def amanda_repo():
    return [amanda_to_twilight, amanda_schedule]
