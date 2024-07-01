from pathlib import Path
from textwrap import dedent

import pandas as pd
import requests

from dagster import (
    Failure,
    Field,
    In,
    Nothing,
    OpExecutionContext,
    Out,
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

from ops.fs import remove_file
from ops.sql_server.file_to_table import file_to_table
from ops.sql_server.truncate_table import truncate_table
from ops.template import apply_substitutions

from resources.mssql import mssql_resource, MSSqlServerResource


@op(
    config_schema={
        "path": Field(
            String,
            description="""Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext.""",
        ),
        "substitutions": Field(
            Permissive(),
            description="Subsitution mapping for substituting in `path`",
            is_required=False,
        ),
    },
    out=Out(String, "The path to the file created by this operation"),
)
def signs_to_file(context: OpExecutionContext):
    session = requests.Session()

    res = session.post(
        "https://pbotapps.portland.gov/graphql/sign",
        json={"query": "{ signs { _id status mutcdCode legend type } }"},
        verify=False,
    )

    signs = res.json().get("data").get("signs", [])

    if not len(signs):
        raise Failure("No signs retrieved!")

    df = (
        pd.DataFrame.from_records(signs)
        .reset_index()
        .rename(columns={"index": "OBJECTID"})
    )

    df["status"] = df["status"].map(
        lambda x: {"in_use": "INUSE", "obsolete": "OBSOLETE"}.get(x, None)
    )

    df["type"] = df["type"].map(lambda x: x if x is not None else [])
    df["type"] = df["type"].map(
        lambda x: sorted(
            [
                {
                    "construction": 1210,
                    "warning": 1270,
                    "regulatory": 1250,
                    "guide": 1220,
                    "parking": 1240,
                    "school": 1260,
                    "pedestrian": 1260,
                    "bike": 1260,
                }.get(t, 0)
                for t in x
            ]
        )
    )
    df["type"] = df["type"].map(lambda x: max(x) if len(x) > 0 else 0)

    df = df.rename(
        columns={
            "_id": "SignCode",
            "legend": "Legend",
            "mutcdCode": "MutcdCode",
            "status": "Status",
            "type": "SignType",
        }
    )

    df["SignTypeDesc"] = df["SignType"].map(
        lambda x: {
            1210: "SIConstructionST",
            1270: "SIWarningST",
            1250: "SIRegulatoryST",
            1220: "SIGuideST",
            1240: "SIParkingST",
            1260: "SISchoolPedBikeST",
        }.get(x, None)
    )

    df["ImagePath"] = df["SignCode"].map(
        "https://pbotapps.portland.gov/sign-library/{}".format
    )

    path = context.op_config["path"]

    if "substitutions" in context.op_config:
        path = apply_substitutions(
            template_string=path,
            substitutions=context.op_config["substitutions"],
            context=context,
        )

    Path(path).parent.resolve().mkdir(parents=True, exist_ok=True)

    df.to_parquet(path, index=False)

    return path


@op(required_resource_keys={"sql_server"}, ins={"start": In(Nothing)})
def refresh_signfaces(context: OpExecutionContext):
    conn: MSSqlServerResource = context.resources.sql_server

    sql = dedent(
        """
    update
        PDOT.SIGNFACE
    set
        ImagePath = l.ImagePath
    from
        PDOT.SIGNFACE as f with (nolock)
    inner join
        PDOT.SIGNLIB as l
    on
        l.SignCode = f.SignCode
    where
        f.ImagePath is null
    """
    )

    conn.execute(
        context=context,
        sql=sql,
    )


@job(
    resource_defs={
        "sql_server": mssql_resource,
        "io_manager": fs_io_manager,
    }
)
def sign_library_to_assets():
    path = file_to_table(signs_to_file(), truncate_table())
    remove_file(path)
    refresh_signfaces(start=path)


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


@repository
def assets_repository():
    return [sign_library_to_assets, sign_library_schedule]
