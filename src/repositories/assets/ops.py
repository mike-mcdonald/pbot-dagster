from datetime import datetime
from pathlib import Path

from subprocess import run
from textwrap import dedent
from typing import Callable, Literal

import pandas as pd
import requests

from dagster import (
    file_relative_path,
    op,
    Array,
    Failure,
    Field,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Permissive,
    List,
    String,
)

from models.connection import get_connection
from ops.template import apply_substitutions
from resources.mssql import MSSqlServerResource


def execute_asset_script(context: OpExecutionContext, *args):
    conn = get_connection(context.op_config["conn_id"])

    exe = context.op_config["python_exe"]
    script = file_relative_path(__file__, context.op_config["script_path"])

    # Call ArcGIS python script with approipriate interpreter and args
    proc = run(
        [
            exe,
            script,
            "--server",
            f"{conn.host}",
            "--database",
            f"{conn.schema}",
            "--username",
            f"{conn.login}",
            "--password",
            f"{conn.password}",
            *args,
        ],
        capture_output=True,
        encoding="utf8",
    )

    if proc.stdout:
        context.log.info(proc.stdout)
    if proc.stderr:
        context.log.error(proc.stderr)

    if proc.returncode > 0:
        raise Failure("Process failed to complete successfully")


@op(
    config_schema={
        "python_exe": Field(String),
        "script_path": Field(String),
        "conn_id": Field(String),
        "feature_class": Field(String),
        "output": Field(String),
    }
)
def extract_asset(context: OpExecutionContext):
    Path(context.op_config["output"]).parent.resolve().mkdir(
        parents=True, exist_ok=True
    )

    execute_asset_script(
        context, context.op_config["feature_class"], context.op_config["output"]
    )

    return context.op_config["output"]


def create_updates_factory(
    name: str,
    keep: Literal["left_only", "both"],
    filter: Callable = None,
    **kwargs,
):

    @op(
        name=name,
        config_schema={
            "index_field": String,
            "retain_fields": Field(Array(str), is_required=False),
            "exclude_fields": Field(Array(str), is_required=False),
            "destination": String,
        },
    )
    def _inner_op(
        context: OpExecutionContext,
        source_path: str,
        sink_path: str,
    ) -> str:
        left = pd.read_json(source_path, orient="records").set_index(
            context.op_config["index_field"]
        )

        if filter is not None:
            left = left[filter(left)]

        right = pd.read_json(sink_path, orient="records").set_index(
            context.op_config["index_field"]
        )

        m = left.merge(
            right, on=context.op_config["index_field"], how="left", indicator=True
        )

        df = left[m["_merge"] == keep]

        if "retain_fields" in context.op_config:
            df = df[context.op_config["retain_fields"]]

        if "exclude_fields" in context.op_config:
            df = df.drop(columns=context.op_config["exclude_fields"])

        df.reset_index().to_json(context.op_config["destination"], orient="records")

        return context.op_config["destination"]

    return _inner_op


@op(
    config_schema={
        "destination": String,
    },
)
def determine_eval_updates(context: OpExecutionContext, path: str) -> str:
    df = pd.read_json(path, orient="records")

    df = df[
        [
            "AssetID",
            "EvaluationType",
            "EvaluationDate",
            "DamageRating",
            "RepairCost",
            "ClosureStatus",
        ]
    ]

    t1 = df[df.EvaluationType == "Tier1"].copy()
    t2 = df[df.EvaluationType == "Tier2"].copy()

    t1["Tier1AssessmentComplete"] = "Y"
    t1["Tier1AssessmentDate"] = t1["EvaluationDate"]

    t2["Tier2InspectionComplete"] = "Y"
    t2["Tier2InspectionDate"] = t2["EvaluationDate"]

    df = df.merge(t1, how="left").merge(t2, how="left")

    grp = df.sort_values(["AssetID", "EvaluationDate"], ascending=False).groupby(
        ["AssetID"]
    )

    df["Tier1AssessmentComplete"] = grp.bfill()["Tier1AssessmentComplete"]
    df["Tier1AssessmentDate"] = grp.bfill()["Tier1AssessmentDate"]
    df["Tier2InspectionComplete"] = grp.bfill()["Tier2InspectionComplete"]
    df["Tier2InspectionDate"] = grp.bfill()["Tier2InspectionDate"]

    df = df.sort_values(["AssetID", "EvaluationDate"], ascending=False)

    grp = df.groupby(["AssetID"])

    df = (
        grp.nth(0)
        .reset_index()
        .drop(
            columns={
                "EvaluationType",
                "EvaluationDate",
            }
        )
    )

    df.to_json(context.op_config["destination"], orient="records")

    return context.op_config["destination"]


@op(
    config_schema={
        "python_exe": Field(String),
        "script_path": Field(String),
        "conn_id": Field(String),
        "feature_class": Field(String),
    }
)
def insert_asset(context: OpExecutionContext, path: str) -> str:
    execute_asset_script(
        context,
        context.op_config["feature_class"],
        path,
    )

    return path


@op(
    config_schema={
        "python_exe": Field(String),
        "script_path": Field(String),
        "conn_id": Field(String),
        "feature_class": Field(String),
    }
)
def update_asset(context: OpExecutionContext, path: str):
    df = pd.read_json(path, convert_dates=[], orient="records")

    conn: MSSqlServerResource = MSSqlServerResource(context.op_config["conn_id"])

    with conn.client.cursor() as cursor:
        for row in df.itertuples(index=False):
            asset_id = row.AssetID
            values = list(row)

            columns = df.columns.tolist()
            geom = None

            if "SHAPE@XY" in columns:
                geom = values.pop(columns.index("SHAPE@XY"))
                columns.remove("SHAPE@XY")

            set_stmts = [f"[{c}] = ?" for c in columns]

            if geom:
                set_stmts.append(
                    f"[Shape] = geometry::STGeomFromText('POINT({geom[0]} {geom[1]})', 2913)"
                )

            stmt = f"""
                update
                    {context.op_config["feature_class"]}
                set
                    {", ".join(set_stmts)}
                where
                    [AssetID] = '{asset_id}'
            """

            cursor.execute(
                stmt,
                *[None if pd.isnull(x) else x for x in values],
            )

    return path


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
