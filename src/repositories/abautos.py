import json
import numpy as np
import os
import pandas as pd
import re
import requests
import textwrap

from typing import Any, Iterable

from dagster import (
    EnvVar,
    Failure,
    Field,
    HookContext,
    In,
    Int,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    Permissive,
    RunRequest,
    ScheduleEvaluationContext,
    String,
    failure_hook,
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
from resources.fs import FileShareResource, fileshare_resource
from resources.mssql import MSSqlServerResource, mssql_resource


@op(
    config_schema={
        "interval": Field(
            Int,
            description="Number of additional minutes from start date to query for data.",
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
        "zendesk_url": Field(
            String, description="The base URL for the Zendesk API instance"
        ),
        "zendesk_key": Field(
            String,
            description="Zendesk API key to pass for authentication",
        ),
    },
    out=Out(
        String,
        "The path to the json file created from calling the Zendesk API",
    ),
)
def fetch_reports(context: OpExecutionContext):
    import truststore

    truststore.inject_into_ssl()

    start_date = datetime.fromisoformat(context.op_config["scheduled_date"])
    end_date = start_date + timedelta(minutes=int(context.op_config["interval"]))

    session = requests.Session()

    def fetch(url: str, params: dict):
        res = session.get(
            url,
            headers={
                "Authorization": f"Bearer {context.op_config['zendesk_key']}",
                "Content-Type": "application/json",
            },
            params=params,
        )

        if res.status_code != 200:
            raise Failure(
                description="Fetch reports error",
                metadata={
                    "url": MetadataValue.url(res.url),
                    "status_code": MetadataValue.int(res.status_code),
                    "text": MetadataValue.text(res.text),
                },
            )

        return res.json()

    res = fetch(
        f"{context.op_config['zendesk_url']}/api/v2/search/export",
        {
            "page[size]": 1000,
            "filter[type]": "ticket",
            "query": f"group_id:18716157058327 ticket_form_id:17751920813847 created>={start_date.isoformat()} created<{end_date.isoformat()}",
        },
    )

    data = res.get("results", [])

    while res.get("meta").get("has_more"):
        res = fetch(res.get("links").get("next"))
        data.append(res.get("results", []))

    path = context.op_config["path"]

    if "substitutions" in context.op_config:
        path = apply_substitutions(
            template_string=path,
            substitutions=context.op_config["substitutions"],
            context=context,
        )

    Path(path).parent.resolve().mkdir(parents=True, exist_ok=True)

    with open(path, "w") as fd:
        json.dump(data, fd)

    if len(data) == 0:
        raise Failure(
            description="No reports retrieved",
            metadata={
                "start_date": MetadataValue.text(str(start_date)),
                "end_date": MetadataValue.text(str(end_date)),
            },
        )

    return path


@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
        "zpath": Field(
            String,
            description="The file location to write the DataFrame to",
        ),
    },
    ins={
        "path": In(String),
    },
    out=Out(
        String,
        "The path to the processed DataFrame",
    ),
)
def read_reports(context: OpExecutionContext, path: str):
    df = pd.read_json(path, orient="records")

    df = df.rename(columns={"id": "Id"})

    def get_zendesk_field(iter: Iterable, id: int):
        return next(filter(lambda f: f.get("id") == id, iter)).get("value")

    df["Occupied"] = df.custom_fields.map(
        lambda x: get_zendesk_field(x, 14510509580823)
    )

    df["Occupied"] = np.select(
        condlist=[df["Occupied"] == True, df["Occupied"] == False],
        choicelist=["YES", "NO"],
        default="UNKNOWN",
    )

    df["report_fields"] = df.custom_fields.map(
        lambda x: json.loads(get_zendesk_field(x, 17698062540823))
    )

    def get_report_field(fields: dict, key: str) -> Any:
        return fields.get(key, {}).get("value")

    df["Color"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_vehicle:color").upper()
    )

    df["Type"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_vehicle:type").upper()
    )

    df["Make"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_vehicle:make").upper()
    )

    df["State"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_vehicle:license_plate_state").upper()
    )

    df["License"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_vehicle:license_plate_number").upper()
    )

    df["Address"] = df.report_fields.map(
        lambda x: get_report_field(x, "report_location:location_address")
        .replace(r"/", "")
        .replace(r"\\", "")
    )

    df["Lat"] = df.report_fields.map(
        lambda x: float(get_report_field(x, "report_location:location_lat"))
    )

    df["Lng"] = df.report_fields.map(
        lambda x: float(get_report_field(x, "report_location:location_lon"))
    )

    df["Names"] = df.report_fields.map(lambda x: get_report_field(x, "contact_name"))

    df["Phone"] = df.report_fields.map(lambda x: get_report_field(x, "contact_phone"))

    df["Email"] = df.report_fields.map(lambda x: get_report_field(x, "contact_email"))

    df["Waived"] = df.report_fields.map(
        lambda x: get_report_field(x, "confidentiality_waiver")
    )
    df["Waived"] = np.select(
        condlist=[
            (df["Waived"] == "I do not waive confidentiality"),
            (df["Waived"] == "I choose to waive confidentiality"),
            (df["Waived"] == "I waive confidentiality"),
        ],
        choicelist=["0", "1", "1"],
        default="0",
    )

    def create_description(fields: dict):
        camp = get_report_field(fields, "report_is_camp")
        camp = f"Camp:{camp}" if camp is not None else ""

        inoperables = " ".join(
            get_report_field(fields, "report_vehicle_inoperable") or []
        ).replace("'", "")

        private = get_report_field(fields, "report_location_is_private")
        private = f"Private:{private}" if private is not None else ""

        details = " ".join(
            get_report_field(fields, "report_location:location_details") or []
        ).replace("'", "")

        attrs = get_report_field(fields, "report_location:location_attributes").strip()

        desc = re.sub(
            r"\s{2,}", " ", " ".join([camp, inoperables, private, details, attrs])
        )

        max_size = 128
        if len(desc) <= max_size:
            desc = desc[:125] + "..."

        return desc

    df["Details"] = df.report_fields.map(create_description)

    def get_area(address: str):
        m = re.match(r" [ENSW]{1,2} ", address)
        return "SE" if m is None else m.group().strip()

    df["Area"] = df["Address"].map(get_area)

    df["FirstName"] = df["Names"].astype(str).str.split().str[0]
    df["LastName"] = df["Names"].astype(str).str.split().str[1]

    df = df.fillna("")

    df = df[
        [
            "Id",
            "Color",
            "Type",
            "Make",
            "License",
            "State",
            "Details",
            "Area",
            "Address",
            "Lat",
            "Lng",
            "FirstName",
            "LastName",
            "Phone",
            "Email",
            "Waived",
            "Occupied",
        ]
    ]

    zpath = context.op_config["zpath"]
    Path(zpath).parent.resolve().mkdir(parents=True, exist_ok=True)
    df.to_parquet(zpath, index=False)

    return zpath


@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
    },
    ins={
        "zpath": In(String),
    },
    out=Out(
        String,
        "The path to the parquet file created with list of cases created",
    ),
    required_resource_keys={"sql_server"},
)
def write_reports(context: OpExecutionContext, zpath: str):
    df = pd.read_parquet(zpath)

    conn: MSSqlServerResource = context.resources.sql_server

    context.log.info(f"🚀 Attempting to add {len(df)} cases to database...")
    trace = datetime.now()
    results = []

    for row in df.itertuples(index=False):
        cursor = conn.execute(
            context, "Exec sp_CreateAbCaseZ ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ", *row
        )
        res = cursor.fetchone()

        if res is None:
            raise Failure("Failed to successfully execute stored procedure")

        (id, status, case_number) = res

        if status == "Success":
            results.append({"Id": row.Id, "AbCaseId": id, "CaseNo": case_number})

        cursor.close()

    context.log.info(f"Created {len(results)} new cases in {datetime.now() - trace}.")

    df = pd.DataFrame.from_records(results)

    if len(df) == 0:
        raise Failure(
            "No data written to database",
        )

    df.to_parquet(zpath, index=False)

    return zpath


@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
    },
    ins={
        "zpath": In(String),
    },
    out=Out(
        String,
        "The path to the parquet file created with list of photo urls",
    ),
)
def get_photo_urls(context: OpExecutionContext, zpath: str):
    import truststore

    truststore.inject_into_ssl()

    df = []

    for row in pd.read_parquet(zpath).itertuples(index=True):
        response = requests.get(
            f"{os.getenv('zendesk_url')}/api/v2/tickets/{row.Id}/comments",
            headers={
                "Authorization": f"Bearer {os.getenv('ZENDESK_API_KEY')}",
                "Content-Type": "application/json",
            },
        )

        if response.status_code != 200:
            raise Exception(
                f"🔥 Error retriving comments: '{response.status_code}' - '{response.text}'"
            )

        comments = response.json().get("comments")

        count = 0
        for comment in comments:
            for attachment in comment["attachments"]:
                if "image" in attachment["content_type"]:
                    df.append(
                        {
                            "Id": row.Id,
                            "AbCaseId": row.AbCaseId,
                            "PhotoUrl": attachment["content_url"],
                            "PhotoFileName": f"{row.CaseNo}-{count}.jpeg",
                        }
                    )
                    count += 1
        context.log.info(
            f"Downloaded {count} photos for ZendeskID {row.Id} and Abcaseid {row.AbCaseId}."
        )

    photoDf = pd.DataFrame.from_records(df)

    photoDf.to_parquet(zpath, index=False)

    if len(photoDf) == 0:
        raise Failure("No photos retrieved")

    return zpath


@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
    },
    ins={
        "zpath": In(String),
    },
    out=Out(
        String,
        description="The dataframe parquet file with list of photos downloaded",
    ),
)
def download_photos(context: OpExecutionContext, zpath: str):
    import shutil
    import truststore

    truststore.inject_into_ssl()

    df = pd.read_parquet(zpath)

    for row in df.itertuples(index=False):
        with requests.get(row.PhotoUrl, stream=True) as r:
            with open(
                f"{context.op_config['parent_dir']}/{row.PhotoFileName}", "wb"
            ) as f:
                shutil.copyfileobj(r.raw, f)

    return zpath


@op(
    ins={
        "zpath": In(String),
    },
    required_resource_keys={"sql_server"},
)
def create_photo_records(context: OpExecutionContext, zpath: str):
    conn: MSSqlServerResource = context.resources.sql_server
    df = pd.read_parquet(zpath)
    count_created = 0
    for row in df.itertuples(index=True, name="Panda"):
        cursor = conn.execute(
            context, "Exec sp_CreateAbCasePhotoZ ?, ? ", row.AbCaseId, row.PhotoFileName
        )
        results = cursor.fetchone()
        if results is None:
            raise Exception
        match results[1]:
            case "Success":
                count_created += 1
            case "Missing":
                context.log.warning(
                    f"🚀 Not found record with caseid - {row.AbCaseId}. Did create abcasephoto record."
                )
        cursor.close()
    context.log.info(
        f"🚀 {count_created} photo records created in Abandoned Autos database."
    )


@op(
    config_schema={
        "destination_dir": Field(
            String,
            description="The abautos image directory location",
        ),
        "parent_dir": Field(
            String,
            description="The download photos directory location",
        ),
    },
    ins={
        "zpath": In(String),
    },
    out=Out(
        String,
        description="The parent dir for removal to cleanup ",
    ),
    required_resource_keys=["photo_share"],
)
def copy_photo_files(context: OpExecutionContext, zpath: str):
    df = pd.read_parquet(zpath)

    share: FileShareResource = context.resources.photo_share

    source_folder = Path(context.op_config["parent_dir"])

    def copyFile(photoFileName: str):
        share.upload(source_folder / photoFileName, photoFileName)

    df.PhotoFileName.apply(copyFile)

    return context.op_config["parent_dir"]


@failure_hook
def remove_dir_on_failure(context: HookContext):
    import shutil

    path = Path(context.op_config["parent_dir"])

    shutil.rmtree(path)


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "sql_server": mssql_resource,
        "photo_share": fileshare_resource,
    },
    hooks={remove_dir_on_failure},
)
def process_zendesk_data():
    path = download_photos(get_photo_urls(write_reports(read_reports(fetch_reports()))))
    # Create abcasephoto record
    create_photo_records(path)
    # Move photos to abautos/images folder
    remove_dir(copy_photo_files(path))


@schedule(
    job=process_zendesk_data,
    cron_schedule="*/5 * * * *",
    execution_timezone="US/Pacific",
)
def zendesk_api_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time

    execution_date_path = f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}{execution_date.strftime('%Y%m%dT%H%M%S')}"

    execution_date = execution_date.isoformat()

    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "photo_share": {
                    "config": {
                        "conn_id": "fs_abautos_photos",
                    }
                },
                "sql_server": {
                    "config": {"mssql_server_conn_id": "mssql_server_abautos"}
                },
            },
            "ops": {
                "fetch_reports": {
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
                "read_reports": {
                    "config": {
                        "parent_dir": execution_date_path,
                        "zpath": f"{execution_date_path}/df.parquet",
                    },
                },
                "write_reports": {
                    "config": {
                        "parent_dir": execution_date_path,
                    },
                },
                "get_photo_urls": {
                    "config": {
                        "parent_dir": execution_date_path,
                    },
                },
                "download_photos": {
                    "config": {
                        "parent_dir": execution_date_path,
                    },
                },
                "copy_photo_files": {
                    "config": {
                        "destination_dir": f"{EnvVar('ABAUTOS_IMAGE_PATH').get_value()}",
                        "parent_dir": execution_date_path,
                    },
                },
            },
        },
    )


@repository
def abautos_repository():
    return [process_zendesk_data, zendesk_api_schedule]
