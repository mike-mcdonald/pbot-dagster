import json
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
from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource, mssql_resource


@op(
    config_schema={
        "interval": Field(
            Int,
            description="Number of years to return date. ",
        ),
        "path": Field(
            String,
            description="""Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext.""",
        ),
        "parent_dir": Field(
            String,
            description="Parent folder for the files created.",
        ),
        "scheduled_date": Field(
            String,
            description="Job scheduled date in isoformat.",
        ),
        "substitutions": Field(
            Permissive(),
            description="Subsitution mapping for substituting in `path`",
            is_required=False,
        ),
    },
    out={
        "stop": Out(
            String,
            "The parent folder",
            is_required=False,
        ),
        "proceed": Out(
            String,
            "The path to the df file containing list of AffidavitID and UID to remove.",
            is_required=False,
        ),
    },
)
def get_Affidavit_list(context: OpExecutionContext):
    conn: MSSqlServerResource = context.resources.sql_server
    interval = context.op_config["interval"]
    query = f"select s.AffidavitId as id, a.AffidavitUID as uid from AffidavitStatus s left join Affidavit a on s.AffidavitID= a.AffidavitID 
    where s.AffidavitStatus = ''RepairsComplete''and s.StatusDate <= DATEADD(year,{interval},GETDATE()) "
    cursor = conn.execute(context, query)
    results = cursor.fetchall()
    if results is None:
        raise Exception
    column_names = [desc[0] for desc in cursor.description
    df = pd.DataFrame(results, columns=column_names)
    print(df)
    cursor.close()
    conn.close()
    context.log.info( f"🚀 {len(df)} record ids found for removal.")

@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
        "path": Field(
            String,
            description="The parquet file location",
        ),
    },
    ins={
        "Ipath": In(String),
    },
)
def remove_Affidavits(context: OpExecutionContext):
     df = pd.read_parquet(path)

def remove Inspections(context: OpExecutionContext):
    df = pd.read_parquet(path)

def remove Permits(context: OpExecutionContext):
     df = pd.read_parquet(path)

def remove Files:  
     df = pd.read_parquet(path)
    # Remove vsdx files . Use uid as filename with filetype of vsdx 
    #\\pbotfile\Apps\SidewalkPosting\Documents\Diagrams
    
    # Remove folders. Folder names is the AffidavitID 
    # \\pbotfile\Apps\SidewalkPosting\Documents\AffidavitFolders

@schedule(
    job=process_retention,
    cron_schedule="*/5 * * * *",
    execution_timezone="US/Pacific",
)
def sidewalk_retention_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time

    execution_date_path = f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}{execution_date.strftime('%Y%m%dT%H%M%S')}"

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
                "remove_Affidavit": {
                    "config": {
                        "interval": 5,
                        "path": f"{execution_date_path}/output.json",
                        "parent_dir": execution_date_path,
                        "scheduled_date": execution_date,
                        "substitutions": {
                            "execution_date": execution_date,
                        },
                        "zendesk_key": EnvVar("ZENDESK_API_KEY").get_value(),
                        "zendesk_url": EnvVar("ZENDESK_URL").get_value(),
                    },
                },
            
            },
        }
    )


@repository
def retention_repository():
    return [process_retention,sidewalk_retention_schedule]
