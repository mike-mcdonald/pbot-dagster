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

from datetime import datetime
from datetime import timedelta
from pathlib import Path

from ops.fs import remove_file, remove_file_config
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
            "The path to the json file created from calling the Zendesk API",
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
        yield Output(path, "stop")
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
        "zpath": Field(
            String,
            description="The parquet file location",
        ),
    },
    ins={
        "path": In(String),
    },
    out=Out(
        String,
        description="The parquet file for input to write_reports",
    ),
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

    zpath = context.op_config["zpath"]
    Path(zpath).parent.resolve().mkdir(parents=True, exist_ok=True)
    df.to_parquet(zpath, index=False)
    return zpath


@op(
    ins={
        "zpath": In(String),
    },
    out=Out(
        String,
        description="The Zendesk dataframe parquet file to remove",
    ),
    required_resource_keys={"sql_server"},
)
def write_reports(context: OpExecutionContext, zpath: str):
    df = pd.read_parquet(zpath)

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
            case "Duplicate":
                context.log.info(
                    f"🚀 Write reports duplicate Zendesk ID - {row[0]}. Caseid - {str(results[0])} already exist."
                )
    context.log.info(
        f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')} Write reports: {count_created} cases created in Abandoned Autos database."
    )
    return zpath


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "sql_server": mssql_resource,
    }
)
def process_zendesk_data():
    stop, proceed = fetch_reports()

    # No data, just remove file
    remove_file(stop)

    # There is data, keep processing
    zpath = read_reports(proceed)

    remove_file(write_reports(zpath))
    remove_file_config(zpath)


@schedule(
    job=process_zendesk_data,
    cron_schedule="*/5 * * * *",
    execution_timezone="US/Pacific",
)
def zendesk_api_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time

    execution_date_path = execution_date.strftime("%Y%m%dT%H%M%S")

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
                        "path": f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}abautos/{execution_date_path}.json",
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
                        "zpath": f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}abautos/{execution_date_path}.parquet",
                    },
                },
                "remove_file_config": {
                    "config": {
                        "path": f"{EnvVar('DAGSTER_SHARE_BASEPATH').get_value()}abautos/{execution_date_path}.json",
                    }
                },
            },
        },
    )


@repository
def abautos_repository():
    return [process_zendesk_data, zendesk_api_schedule]
