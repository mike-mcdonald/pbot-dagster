import json
import pathlib
import numpy as np
import os
import pandas as pd
import pyodbc
import re
import requests
import textwrap

from dagster import (
    EnvVar,
    Field,
    In,
    Int,
    OpExecutionContext,
    Out,
    Output,
    Permissive,
    RunRequest,
    ScheduleEvaluationContext,
    String,
    fs_io_manager,
    job,
    op,
    repository,
    schedule,
)

from datetime import datetime, timedelta
from pathlib import Path

from ops.fs import remove_dir
from ops.fs.remove import remove_file
from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource, mssql_resource


@op( required_resource_keys={"sql_server"},)
def remove_database_data(context: OpExecutionContext, dataframe: pd.DataFrame):
    df = dataframe
    count = 0
    conn: MSSqlServerResource = context.resources.sql_server
    for row in df.itertuples(index=False, name=None):
        cursor = conn.execute(
            context, "Exec sp_retention ?", row[0]
        )
        results = cursor.fetchone()
        if results is None:
            raise Exception
        match results[0]:
            case "Success":
                context.log.info(
                    f"🚀 Delete successfully for ID - {row[0]}."
                )
                count += 1
            case _  :
                context.log.info(
                    f"🚀 Failed deleting ID - {row['AffidavitId']}."
                )
        cursor.close()
    context.log.info(
        f"🚀 Remove_database_data {count} "
    )

@op(
    config_schema={
        "diagrams_path": Field(
            String,
            description="Path to visio diagrams",
        ),
        "folders_path": Field(
            String,
            description="Path to affidavit folders",
        ),
    },
)
def remove_network_files(context: OpExecutionContext, dataframe: pd.DataFrame):
    df = dataframe
    diagrams_path = context.op_config["diagrams_path"]
    folders_path = context.op_config["folders_path"]
    context.log.info(
        f"🚀 Diagrams path {diagrams_path} "
    )
    context.log.info(
        f"🚀 Folders path {folders_path} "
    )
    df["AffidavitUID"].apply(lambda x: remove_file(f"{diagrams_path}{x}.vsdx"))
    df["AffidavitId"].apply(lambda x: remove_dir(f"{folders_path}{x}"))
    context.log.info(
        f"🚀 Remove_network_files {df.head(5)} "
    )
@op(
    config_schema={
        "interval": Field(
            Int,
            description="Number of years to return date. Should be NEGATIVE number",
        ),
    },
    out=Out(pd.DataFrame),
    required_resource_keys={"sql_server"},
)
def get_removal_list(context: OpExecutionContext):
    conn: MSSqlServerResource = context.resources.sql_server
    interval = context.op_config["interval"]

    query = f"select s.AffidavitId, a.AffidavitUID from AffidavitStatus s left join Affidavit a on s.AffidavitID= a.AffidavitID where s.AffidavitStatus = 'RepairsComplete' and s.StatusDate <= DATEADD(year,{interval},GETDATE())"

    df = pd.read_sql(query, conn.get_connection())

    context.log.info( f"🚀 {len(df)} record ids found for removal.")

    return(df.head(50))

@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "sql_server": mssql_resource,
    }
)
def process_retention():
    df = get_removal_list()
    remove_database_data(df)
    remove_network_files(df)

@schedule(
    job=process_retention,
    cron_schedule="* * * * 7",
    execution_timezone="US/Pacific",
)
def sidewalk_retention_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time
    execution_date = execution_date.isoformat()
    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "sql_server": {
                    "config": {"mssql_server_conn_id": "mssql_server_sidewalk"}
                },
            },
            "ops": {
                "get_removal_list": {
                    "config": {
                        "interval": 5,
                    },
                },
                "remove_network_files": {
                    "config": {
                        "diagrams_path": f"//pbotfile/Apps/SidewalkPosting/Document/Diagrams/",
                        "folders_path": f"//pbotfile/Apps/SidewalkPosting/Documents/AffidavitFolders/",
                    },
                },
            },
        }
    )


@repository
def retention_repository():
    return [process_retention,sidewalk_retention_schedule]
