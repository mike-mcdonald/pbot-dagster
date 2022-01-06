from dagster import (
    In,
    List,
    OpExecutionContext,
    String,
    fs_io_manager,
    job,
    op,
    repository,
    schedule,
)

from ops.azure import upload_file
from ops.fs import remove_files
from ops.sql_server import get_table_names, table_to_csv

from resources import adls2_resource
from resources.mssql import mssql_resource


@job(
    resource_defs={
        "adls2_resource": adls2_resource,
        "sql_server": mssql_resource,
        "io_manager": fs_io_manager,
    }
)
def tsup_to_twilight():
    files = get_table_names().map(table_to_csv).map(upload_file)

    remove_files(files.collect())


@schedule(
    job=tsup_to_twilight,
    cron_schedule="0 0 * * *",
    execution_timezone="US/Pacific",
)
def tsup_schedule(context):
    execution_date = context.scheduled_execution_time.strftime("%Y%m%dT%H%M%S")
    return {
        "resources": {
            "adls2_resource": {
                "config": {
                    "azure_data_lake_gen2_conn_id": "azure_data_lake_gen2",
                }
            },
            "sql_server": {"config": {"mssql_server_conn_id": "mssql_server_tsup"}},
        },
        "ops": {
            "get_table_names": {"config": {"schema": "TSUP"}},
            "table_to_csv": {
                "config": {
                    "schema": "TSUP",
                    "path": "//pbotdm1/pudl/tsup/${execution_date}/${table}.csv",
                    "substitutions": {"execution_date": execution_date},
                }
            },
            "upload_file": {
                "config": {
                    "container": "twilight",
                    "remote_path": "dagster/${pipeline_name}/${name}/${execution_date}${suffix}",
                    "substitutions": {"execution_date": execution_date},
                }
            },
        },
    }


@repository
def tsup_repo():
    return [tsup_to_twilight, tsup_schedule]
