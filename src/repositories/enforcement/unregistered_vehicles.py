import os

from datetime import datetime
from pathlib import Path

from dagster import (
    Failure,
    Field,
    List,
    OpExecutionContext,
    Out,
    String,
    fs_io_manager,
    job,
    op,
)


from models.connection import Connection
from ops.fs.remove import remove_file, remove_files
from ops.sftp.download import download_list
from ops.sftp.list import list as list_dir
from ops.sftp.delete import delete_list
from resources.mssql import mssql_resource
from repositories.enforcement.violation_types import VIOLATION_TYPE_MAP
from resources.ssh import ssh_resource


@op(
    config_schema={"output": Field(String)},
    out={"path": Out(String), "files": Out(List)},
)
def create_dataframe(
    context: OpExecutionContext, files: list[str]
) -> tuple[str, list[str]]:
    from hashlib import sha256

    import geopandas as gpd
    import numpy as np
    import pandas as pd
    import requests

    from shapely.geometry import Point

    context.log.info(f"Concatenating {len(files)} files into dataframe...")

    df = pd.concat([pd.read_csv(f, index_col=False) for f in files])

    context.log.info(f"Concatenated dataframe has {len(df)} records!")

    df["Hash"] = df.apply(
        lambda x: sha256(
            ":".join(
                [
                    str(v)
                    for v in [
                        x["Ticket status"],
                        x["Offense 1"],
                        x.Officer,
                        x["From (Date-Time)"],
                    ]
                ]
            ).encode("utf8")
        ).hexdigest(),
        axis="columns",
    )

    df["DateTime"] = df.apply(
        lambda x: (
            x["To (Date-Time)"]
            if x["To (Date-Time)"].split(" ")[0].split("-")[0] != "0001"
            else x["From (Date-Time)"]
        )
    )
    df = df[
        [
            "Hash",
            "Ticket status",
            "Offense 1",
            "Beat",
            "Officer",
            "DateTime",
            "Amount",
            "GPS",
        ]
    ].rename(
        columns={
            "Ticket status": "Status",
            "Offense 1": "ViolationNumber",
        }
    )

    df = df[df["Status"] != "T"]
    df = df[~df["Officer"].isin(["999", "XX01", "XX99", "DEFAULT"])]

    df = df.drop_duplicates(subset="Hash")

    df["Status"] = df["Status"].astype("category")

    df["DateTime"] = df["DateTime"].map(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    )

    df["ViolationCode"] = df["ViolationNumber"].map(
        lambda x: VIOLATION_TYPE_MAP.get(str(x).removesuffix(".0"), dict()).get("Code")
    )
    df["ViolationName"] = df["ViolationNumber"].map(
        lambda x: VIOLATION_TYPE_MAP.get(str(x).removesuffix(".0"), dict()).get("Name")
    )

    df["Officer"] = df["Officer"].astype(np.float64)

    df["Longitude"] = df["GPS"].map(lambda x: x.split(";")[1])

    df["Latitude"] = df["GPS"].map(lambda x: x.split(";")[0])

    df["geometry"] = list(zip(df["Longitude"], df["Latitude"]))
    df["geometry"] = df["geometry"].map(Point)

    df = gpd.GeoDataFrame(df, crs="EPSG:4326").to_crs("EPSG:3857")

    df["X"] = df.geometry.map(lambda x: x.x if not x.is_empty else None)
    df["Y"] = df.geometry.map(lambda x: x.y if not x.is_empty else None)

    url = "https://portlandmaps.com/arcgis/rest/services/Public/PBOT_Planning/MapServer/36"

    res = requests.get(f"{url}?f=json")

    extent = res.json()["extent"]

    res = requests.post(
        f"{url}/query",
        {"geometry": extent, "f": "geojson", "OutFields": ["TRACT"]},
    )

    eqm = gpd.GeoDataFrame.from_features(res.json(), crs="EPSG:4326").to_crs(
        "EPSG:3857"
    )

    df = gpd.sjoin(df, eqm, how="left", predicate="intersects")
    df = df.drop(columns=["index_right"]).rename(columns={"TRACT": "CensusTract"})

    df["CensusTract"] = df["CensusTract"].astype("category")

    df = df[
        [
            "Hash",
            "Status",
            "ViolationNumber",
            "ViolationCode",
            "ViolationName",
            "Beat",
            "Officer",
            "CensusTract",
            "DateTime",
            "Amount",
            "X",
            "Y",
        ]
    ]

    # This allows null values within otherwise integer columns
    # Later, we need to account for the special pd.NA value used in these types
    df["ViolationNumber"] = df["ViolationNumber"].map(float).astype("Int64")
    df["Beat"] = df["Beat"].astype("Int64")
    df["Officer"] = df["Officer"].astype("Int64")

    Path(context.op_config["output"]).resolve().parent.mkdir(
        parents=True, exist_ok=True
    )

    df.to_parquet(context.op_config["output"], index=False)

    return context.op_config["output"], files


@op(
    required_resource_keys={"geodatabase"},
    config_schema={"feature_class": Field(String)},
)
def append_features(context: OpExecutionContext, path: str) -> str:
    from tempfile import TemporaryDirectory

    import pandas as pd

    import arcpy
    from arcpy.da import InsertCursor
    from arcpy.management import CreateDatabaseConnection

    db: Connection = context.resources.geodatabase.get_connection(
        context.resources.geodatabase.conn_id
    )

    df = pd.read_parquet(path)

    fields = df.columns.to_list()
    fields.remove("X")
    fields.remove("Y")

    with TemporaryDirectory() as tdir:
        sde = CreateDatabaseConnection(
            tdir,
            "db",
            "SQL_SERVER",
            db.host,
            "DATABASE_AUTH",
            username=db.login,
            password=db.password,
            database=db.schema,
        )

        fc = os.path.join(str(sde), context.op_config["feature_class"])

        if not arcpy.Exists(fc):
            raise Failure(
                "The featureclass {} doesn't exist and must be created!".format(
                    context.op_config["feature_class"]
                )
            )

        def safe_get_value(value):
            if value is pd.NA:
                return None
            else:
                return value

        with InsertCursor(fc, ["SHAPE@XY", *fields]) as cursor:
            for row in df.itertuples(index=False):
                cursor.insertRow(
                    [
                        (row.X, row.Y),
                        *[safe_get_value(row.__getattribute__(f)) for f in fields],
                    ]
                )

    return path


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "geodatabase": mssql_resource,
        "sftp": ssh_resource,
    }
)
def process_politess_exports():
    remote_files = list_dir()
    local_files = download_list(remote_files)
    df, files = create_dataframe(local_files)
    remove_file(append_features(df))
    remove_files(files)
    delete_list(remote_files, local_files)
