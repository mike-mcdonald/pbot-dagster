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
    out={
        "stop": Out(
            String,
            "The parent folder",
            is_required=False,
        ),
        "proceed": Out(
            String,
            "The path to the json file created from calling the Zendesk API",
            is_required=False,
        ),
    },
)
def fetch_reports(context: OpExecutionContext):
    api_key = context.op_config["zendesk_key"]
    interval = context.op_config["interval"]
    scheduled_date = context.op_config["scheduled_date"]

    start_date = datetime.fromisoformat(scheduled_date)
    end_date = start_date + timedelta(minutes=int(interval))

    session = requests.Session()

    def get_exports(url):
        res = session.get(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            verify=False,
        )

        if res.status_code != 200:
            raise Exception(
                f"🔥 Fetch reports error '{res.status_code}' - '{res.text}'..."
            )

        return res.json()

    res = get_exports(
        f"{context.op_config['zendesk_url']}/api/v2/search/export?page[size]=1000&filter[type]=ticket&query=group_id:18716157058327 ticket_form_id:17751920813847 created>={start_date.isoformat()} created<{end_date.isoformat()}"
    )

    data = res.get("results", [])

    while res.get("meta").get("has_more"):
        res = get_exports(res.get("links").get("next"))
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
        yield Output(context.op_config["parent_dir"], "stop")
    else:
        yield Output(path, "proceed")


def area_searcher(search_str: str, search_list: str):
    search_obj = re.search(search_list, search_str)
    if search_obj:
        return_str = search_str[search_obj.start() : search_obj.end()].strip()
    else:
        return_str = "SE"
    return return_str


@op(
    config_schema={
        "parent_dir": Field(
            String,
            description="The parent directory location",
        ),
        "zpath": Field(
            String,
            description="The parquet file location",
        ),
    },
    ins={
        "path": In(String),
    },
    out={
        "stop": Out(
            String,
            "The parent dir to remove when there is no cases to process",
            is_required=False,
        ),
        "proceed": Out(
            String,
            "The path to the parquet file created with list of cases created",
            is_required=False,
        ),
    },
)
def read_reports(context: OpExecutionContext, path: str):
    with open(path, "r") as file:
        data: list = json.load(file)

    # this indicates it is an abautos zendesk report
    abautos_details_key_value = 17698062540823
    # indicates it is the occupied field
    abautos_occupied_key_value = 14510509580823
    # To find the area, this list's values has leading and trailing blanks so that
    # it finds the individual area and not as part of a word. So, do not remove the blanks.
    area_list = [" E ", " N ", " NE ", " NW ", " S ", " SE ", " SW ", " W "]
    areapattern = "|".join(area_list)

    column_name = [
        "Id",
        "Color",
        "Type",
        "Make",
        "State",
        "License",
        "Detail",
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
        "Names",
    ]

    (
        id,
        vehColor,
        vehType,
        vehMake,
        vehState,
        vehLicense,
        detail,
        area,
        address,
        lat,
        lng,
        firstName,
        lastName,
        phone,
        email,
        waived,
        occupied,
        names,
    ) = ([], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [])

    for report_record in data:
        zendeskId = str(report_record["id"])
        details = ""
        ## the main data we are looking is on the value property
        ## of one of many custom field id/value pairs
        custom_fields = report_record["custom_fields"]
        occupied_field = [
            r for r in custom_fields if r["id"] == abautos_occupied_key_value
        ][0]["value"]
        ## filter custom fields to find the one for AbAutos Reports
        ## and return just the value pair for it as json
        abautos_report_fields = json.loads(
            [r for r in custom_fields if r["id"] == abautos_details_key_value][0][
                "value"
            ]
        )

        id.append(zendeskId)
        vehColor.append(abautos_report_fields["report_vehicle:color"]["value"].upper())
        vehType.append(abautos_report_fields["report_vehicle:type"]["value"].upper())
        vehMake.append(abautos_report_fields["report_vehicle:make"]["value"].upper())
        vehState.append(
            abautos_report_fields["report_vehicle:license_plate_state"]["value"].upper()
        )
        vehLicense.append(
            abautos_report_fields["report_vehicle:license_plate_number"][
                "value"
            ].upper()
        )
        address.append(
            abautos_report_fields["report_location:location_address"]["value"]
            .replace(r"/", "")
            .replace(r"\\", "")
        )
        lat.append(abautos_report_fields["report_location:location_lat"]["value"])
        lng.append(abautos_report_fields["report_location:location_lon"]["value"])

        if "contact_name" in abautos_report_fields:
            names.append(abautos_report_fields["contact_name"]["value"])
        if "contact_phone" in abautos_report_fields:
            phone.append(abautos_report_fields["contact_phone"]["value"])
        if "contact_email" in abautos_report_fields:
            email.append(abautos_report_fields["contact_email"]["value"])
        if "confidentiality_waiver" in abautos_report_fields:
            waived.append(abautos_report_fields["confidentiality_waiver"]["value"])

        occupied.append(occupied_field)

        # Details consist of the following fields
        # Need to check if exists to take care of KeyError
        # for all the keys below.
        if "report_is_camp" in abautos_report_fields:
            camp = str(abautos_report_fields["report_is_camp"]["value"]).strip()
            if len(camp) > 0:
                details = details + "Camp:" + camp + " "

        if "report_vehicle_inoperable" in abautos_report_fields:
            inoperables = (
                str(abautos_report_fields["report_vehicle_inoperable"]["value"])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )
            if len(inoperables) > 0:
                details = details + inoperables + " "

        if "report_location_is_private" in abautos_report_fields:
            isprivate = abautos_report_fields["report_location_is_private"][
                "value"
            ].strip()
            if len(isprivate) > 0:
                details = details + "Private:" + isprivate + " "

        if "report_location:location_details" in abautos_report_fields:
            locdetails = (
                str(abautos_report_fields["report_location:location_details"]["value"])
                .strip()
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )
            if len(locdetails) > 0:
                details = details + locdetails + " "

        if "report_location:location_attributes" in abautos_report_fields:
            locattr = (
                str(
                    abautos_report_fields["report_location:location_attributes"][
                        "value"
                    ]
                )
                .strip()
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )
            if len(locattr) > 0:
                details = details + locattr + " "

        # Need to truncate details string because the stored procedure fails if max string length is > 128 char
        max_size = 128
        if len(details) <= max_size:
            description = details
        else:
            description = textwrap.wrap(details, max_size - 3)[0] + "..."
        detail.append(description)

    df = pd.DataFrame(
        [
            id,
            vehColor,
            vehType,
            vehMake,
            vehState,
            vehLicense,
            detail,
            area,
            address,
            lat,
            lng,
            firstName,
            lastName,
            phone,
            email,
            waived,
            occupied,
            names,
        ]
    ).T
    df.columns = column_name

    df["Area"] = df["Address"].apply(
        lambda x: area_searcher(search_str=x, search_list=areapattern)
    )
    df["FirstName"] = df["Names"].astype(str).str.split().str[0]
    df["LastName"] = df["Names"].astype(str).str.split().str[1]
    df["Waived"] = np.select(
        condlist=[
            (df["Waived"] == "I do not waive confidentiality"),
            (df["Waived"] == "I choose to waive confidentiality"),
            (df["Waived"] == "I waive confidentiality"),
        ],
        choicelist=["0", "1", "1"],
        default="0",
    )

    df["Occupied"] = np.select(
        condlist=[df["Occupied"] == True, df["Occupied"] == False],
        choicelist=["YES", "NO"],
        default="UNKNOWN",
    )

    df = df.fillna("")
    df = df.drop("Names", axis=1)
    count_reports = len(df.index)
    context.log.info(
        f"🚀{datetime.now().strftime('%Y-%m-%d %H:%M')}: Read {count_reports} Zendesk Abandoned Autos reports."
    )

    if len(df) == 0:
        yield Output(context.op_config["parent_dir"], "stop")
    else:
        zpath = context.op_config["zpath"]
        Path(zpath).parent.resolve().mkdir(parents=True, exist_ok=True)
        df.to_parquet(zpath, index=False)
        yield Output(zpath, "proceed")
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
    out={
        "stop": Out(
            String,
            "The parent dir to remove when there is no more cases to process",
            is_required=False,
        ),
        "proceed": Out(
            String,
            "The path to the parquet file created with list of cases created",
            is_required=False,
        ),
    },
    required_resource_keys={"sql_server"},
)
def write_reports(context: OpExecutionContext, zpath: str):
    df = pd.read_parquet(zpath)
    (caseId, caseNo) = ([], [])

    count_created = 0

    conn: MSSqlServerResource = context.resources.sql_server

    for row in df.itertuples(index=False, name=None):
        cursor = conn.execute(
            context, "Exec sp_CreateAbCaseZ ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ", *row
        )
        results = cursor.fetchone()
        if results is None:
            raise Exception
        match results[1]:
            case "Success":
                context.log.info(
                    f"🚀 Write reports successfully for Zendesk ID - {row[0]} with caseid - {str(results[0])}."
                )
                count_created += 1
                caseId.append(str(results[0]))
                caseNo.append(str(results[2]))
            case "Duplicate":
                context.log.info(
                    f"🚀 Write reports duplicate Zendesk ID - {row[0]}. Caseid - {str(results[0])} already exist."
                )
                caseId.append("Duplicate")
                caseNo.append("0")
            case _:
                caseId.append("CatchAll")
                caseNo.append("0")
        cursor.close()
    context.log.info(
        f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} Write reports: {count_created} cases created in Abandoned Autos database."
    )
    context.log.info(f"🚀 Before: {df.to_string()} ")
    df["AbCaseId"], df["CaseNo"] = [caseId, caseNo]

    context.log.info(f"🚀 After: {df.to_string()} ")
    # Remove records which are 'Duplicate' or 'Catchall'
    mask = df["AbCaseId"].isin(["Duplicate", "CatchAll"])
    df = df[~mask]
    if len(df) == 0:
        yield Output(context.op_config["parent_dir"], "stop")
    else:
        # Keep only columns Id, AbCaseId, CaseNo
        cols_to_keep = ["Id", "AbCaseId", "CaseNo"]
        df.drop(columns=df.columns.difference(cols_to_keep), inplace=True)
        df.to_parquet(zpath, index=False)
        yield Output(zpath, "proceed")


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
    out={
        "stop": Out(
            String,
            "The parent dir to remove because there are no photos",
            is_required=False,
        ),
        "proceed": Out(
            String,
            "The path to the parquet file created with list of photo urls",
            is_required=False,
        ),
    },
)
def get_photo_urls(context: OpExecutionContext, zpath: str):
    import truststore

    truststore.inject_into_ssl()

    df = pd.read_parquet(zpath)
    context.log.info(f"🚀 {df.to_string()}")
    (id, abCaseId, photoUrl, photoFileName) = ([], [], [], [])

    headers = {
        "Authorization": f"Bearer {os.getenv('zendesk_key')}",
        "Content-Type": "application/json",
    }

    for row in df.itertuples(index=True, name="Pandas"):
        url = f"{os.getenv('zendesk_url')}/api/v2/tickets/{row.Id}/comments"
        response = requests.get(
            url,
            headers=headers,
            verify=False,
        )

        if response.status_code != 200:
            raise Exception(
                f"🔥 Fetch photos error '{response.status_code}' - '{response.text}'..."
            )

        comments = response.json().get("comments")
        count = 1
        for comment in comments:
            attachments = comment["attachments"]
            for attachment in attachments:
                if "image" in attachment["content_type"]:
                    photoUrl.append(attachment["content_url"])
                    photoFileName.append(f"{row.CaseNo}-{count}.jpeg")
                    id.append(row.Id)
                    abCaseId.append(row.AbCaseId)
                    context.log.info(
                        f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} Photo url: {attachment['content_url']} Photo filename: {row.CaseNo}-{count}.jpeg"
                    )
                    count += 1
        context.log.info(
            f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} {count-1} photo for ZendeskID {row.Id} and Abcaseid {row.AbCaseId}."
        )

    photoDf = pd.DataFrame(
        [
            id,
            abCaseId,
            photoUrl,
            photoFileName,
        ]
    ).T
    photoDf.columns = ["Id", "AbCaseId", "PhotoUrl", "PhotoFileName"]

    photoDf.to_parquet(zpath, index=False)
    context.log.info(f"🚀 {photoDf.to_string()}")
    if len(photoDf) == 0:
        yield Output(context.op_config["parent_dir"], "stop")
    else:
        yield Output(zpath, "proceed")


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
    required_resource_keys={"sql_server"},
)
def download_photos(context: OpExecutionContext, zpath: str):
    import shutil
    import truststore

    truststore.inject_into_ssl()
    df = pd.read_parquet(zpath)
    context.log.info(f"🚀 Download photo dataframe: {df.to_string()}")
    for row in df.itertuples(index=False, name="Panda"):
        with requests.get(row.PhotoUrl, stream=True) as r:
            with open(
                f"{context.op_config['parent_dir']}/{row.PhotoFileName}", "wb"
            ) as f:
                shutil.copyfileobj(r.raw, f)
                context.log.info(
                    f"🚀 Download photo: {context.op_config['parent_dir']}/{row.PhotoFileName}"
                )
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
                context.log.info(
                    f"🚀 Successfully create abcasephoto record for file '{str(results[0])}' with caseid - {row.AbCaseId}."
                )
                count_created += 1
            case "Missing":
                context.log.info(f"🚀 Not found record with caseid - {row.AbCaseId}.")
        cursor.close()
    context.log.info(
        f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} {count_created} abcasephoto record(s) created."
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
    required_resource_keys={"sql_server"},
)
def copy_photo_files(context: OpExecutionContext,  zpath: str):
    import shutil

    df = pd.read_parquet(zpath)
    source_folder = Path(context.op_config["parent_dir"])
    destination_folder = Path(context.op_config["destination_dir"])

    def copyFile (photoFileName: str):
         shutil.copy(source_folder / photoFileName, destination_folder)
         context.log.info(
             f"🚀 Copied: {source_folder/photoFileName} to {destination_folder}"

        )
        

    df.PhotoFileName.apply(copyFile)
   
    context.log.info(
        f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} photo(s) moved."
    )
    return context.op_config["parent_dir"]


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "sql_server": mssql_resource,
    }
)
def process_zendesk_data():
    files_deleted = list[str]

    stop, proceed = fetch_reports()
    # No data, just remove parent dir
    files_deleted = remove_dir(stop)

    # There is data, keep processing
    stop, proceed = read_reports(proceed)
    files_deleted = remove_dir(stop)

    stop, proceed = write_reports(proceed)
    # No new cases to process
    files_deleted = remove_dir(stop)

    # Proceed to check for photos for new cases
    stop, proceed = get_photo_urls(proceed)
    # No photos to process
    files_deleted = remove_dir(stop)
    # There is photos to download
    zpath = download_photos(proceed)
    # Create abcasephoto record
    create_photo_records(zpath)
    # Move photos to abautos/images folder
    files_deleted = remove_dir(
        copy_photo_files(zpath)
    )


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
                        "zpath": f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}{execution_date_path}/df.parquet",
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
