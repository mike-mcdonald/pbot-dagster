import os

from datetime import datetime
from hashlib import sha256
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
from ops.fs.list import list_dir
from ops.fs.remove import remove_file, remove_files
from resources.mssql import mssql_resource
from repositories.enforcement.violation_types import VIOLATION_TYPE_MAP
from resources.ssh import SSHClientResource, ssh_resource


@op(
    config_schema=
    {"remote_path": Field( String, description="Remote base path to search for files")},
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def get_citations(context: OpExecutionContext):
    import stat
    ssh_client: SSHClientResource = context.resources.ssh_client

    files = []

    try:
        for file in ssh_client.list_iter(context.op_config["remote_path"]):
            # Ignore directories
            if stat.S_ISDIR(file.st_mode):
                continue
            if Path(file.filename).suffix == ".CSV":
                files.append(os.path.join(context.op_config["remote_path"], file.filename))
    except Exception as err:
        context.log.error(f"Error listing SFTP citation files!")
        raise err

    context.log.info(f" 👮👮👮 Found {len(files)} citation csv files in {context.op_config['remote_path']} directory.")
    return files


@op(
    config_schema={
        "remote_path": Field( String, description="Remote base path to search for files"),
        "local_path": Field(String, description="Download files to this location")
    },
    out={"sftp_files": Out(List), "files": Out(List)},
    required_resource_keys=["ssh_client"],
)

def download_citations(context: OpExecutionContext, files: list[str]):

    trace = datetime.now()
    downloads = []
    ssh_client: SSHClientResource = context.resources.ssh_client
    try:
        for remote_file in files:
            local_path = os.path.join(context.op_config["local_path"],Path(remote_file).name)
            Path(local_path).resolve().parent.mkdir(parents=True, exist_ok=True)

            ssh_client.download(remote_file, local_path)
            downloads.append(local_path)
            context.log.info(f"👮 Download {remote_file} to {str(local_path)}")
    except Exception as err:
        context.log.info(f"👮Error downloading {remote_file} to {str(local_path)} ")
        raise err
    context.log.info(f"👮👮👮 Downloaded {len(files)} files took {datetime.now() - trace}")

    return files, downloads

@op(
    required_resource_keys=["ssh_client"],
)

def remove_ftpfiles(context: OpExecutionContext, sftp_list: list[str]):
    ssh_client: SSHClientResource = context.resources.ssh_client
    trace = datetime.now()
    try:
        for file in sftp_list:
            ssh_client.remove(file)
            context.log.info(f"👮 Removing {file}")
    except Exception as err:
        context.log.info(f"👮 Error removing {file}")
        raise err
    context.log.info(f"👮👮👮 Deleted {len(sftp_list)} files took {datetime.now() - trace}")


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
                        x["To (Date-Time)"],
                    ]
                ]
            ).encode("utf8")
        ).hexdigest(),
        axis="columns",
    )

    df = df[
        [
            "Hash",
            "Ticket status",
            "Offense 1",
            "Beat",
            "Officer",
            "To (Date-Time)",
            "Amount",
            "GpsLongitude",
            "GpsLatitude",
        ]
    ].rename(
        columns={
            "Ticket status": "Status",
            "Offense 1": "ViolationNumber",
            "Beat": "Beat",
            "Officer": "Officer",
            "To (Date-Time)": "DateTime",
            "Amount": "Amount",
            "GpsLatitude": "Latitude",
            "GpsLongitude": "Longitude",
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


@job(resource_defs={"io_manager": fs_io_manager, "geodatabase": mssql_resource, "ssh_client": ssh_resource})
def process_politess_exports():
    files = get_citations()
    sftp_files, files = download_citations(files)
    df, files = create_dataframe(files)
    remove_file(append_features(df))
    remove_files(files)
    remove_ftpfiles(sftp_files)
