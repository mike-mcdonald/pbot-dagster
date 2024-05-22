from shutil import rmtree
import numpy as np
import pandas as pd

from dagster import (
    Field,
    Int,
    OpExecutionContext,
    Out,
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
from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource, mssql_resource
from pathlib import Path

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
                    f"🚀 Delete ID - {row[0]} successful."
                )
                count += 1
            case _  :
                context.log.info(
                    f"🚀 Failed deleting ID - {row[0]}."
                )
        cursor.close()
    context.log.info(
        f"🚀 {count} AffidavitIds deleted "
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
        "pdf_path": Field(
            String,
            description="Path to pdf folder",
        ),
    },
)
def remove_network_files(context: OpExecutionContext, dataframe: pd.DataFrame):

    df = dataframe
    diagrams_path = context.op_config["diagrams_path"]
    folders_path = context.op_config["folders_path"]
    pdf_path = context.op_config["pdf_path"]
    for row in df.itertuples(index=False, name=None):
        file_path = Path(f"{diagrams_path}{row[1]}.vsdx")
        if file_path.exists():
            try:
                file_path.unlink(missing_ok=True)
            except Exception as err:
                context.log.info(
                    f"🚮 Error {err} while removing visio diagram {file_path}"
                )
        dir_path = Path(f"{folders_path}{row[0]}")
        if dir_path.exists():
            try:
                rmtree(dir_path)
            except OSError as err:
                context.log.info(
                    f" 🚮 Error: {err.filename} - {err.strerror}"
                )
        file_wildcard = f"{row[0]}*.pdf"
        for p in Path(pdf_path).glob(file_wildcard):
            context.log.info(
                    f"🚮 for loop {p}"
                    )
            if p.exists():
                try:
                    p.unlink(missing_ok=True)
                    context.log.info(
                    f"🚮 Removing pdf files {p}"
                    )
                except Exception as err:
                    context.log.info(
                    f"🚮 Error {err} while removing pdf files {p}"
                    )
    context.log.info(
        f"🚮 Remove_network_files {df} "
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
    return(df)


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
                        "interval": -2,
                    },
                },
                "remove_network_files": {
                    "config": {
                        "diagrams_path": f"//pbotfile1/apps/SidewalkPosting/Documents/TestRetention/Diagrams/",
                        "folders_path": f"//pbotfile1/apps/SidewalkPosting/Documents/TestRetention/AffidavitFolders/",
                        "pdf_path": f"//pbotfile1/apps/SidewalkPosting/Documents/TestRetention/PDF/",
                    },
                },
            },
        }
    )


@repository
def retention_repository():
    return [process_retention,sidewalk_retention_schedule]
