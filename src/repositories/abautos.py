import json
import os

from pathlib import Path

import pandas as pd
import pyodbc
import requests

from dotenv import load_dotenv

from dagster import (
    Field,
    In,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleEvaluationContext,
    String,
    fs_io_manager,
    job,
    op,
    schedule,
)

from ops.fs import remove_file
from resources.mssql import mssql_resource


@op(
    config_schema={
        "path": Field(
            String,
            description="""Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext.""",
        )
    },
    ins={
        "zendesk_api_endpoint": In(String),
    },
    out=Out(String, "The path to the file created by this operation"),
)
def fetch_reports(context: OpExecutionContext):

    zendesk_api_endpoint = context.op_config["zendesk_api_endpoint"]
    scheduled_date = context.op_config["scheduled_date"]
    path = context.op_config["path"]

    context.log.info(f"🚀 Scheduled_date: '{scheduled_date}'")
    # context.log.info(f"🚀 Next_url: '{next_url}'")
    context.log.info(f"🚀 Path: '{path}'")

    load_dotenv()
    api_key = os.getenv("API_KEY")

    # abautos_api_url = "https://portlandoregon.zendesk.com/api/v2/search/export?page[size]=1&filter[type]=ticket&query=group_id:18716157058327&ticket_form_id:17751920813847&created>"+scheduled_date
    abautos_api_url = zendesk_api_endpoint
    context.log.info(f"🚀 Fetch reports using url: '{abautos_api_url}'")

    session = requests.Session()
    response = session.get(
        abautos_api_url,
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        verify=False,
    )

    if response.status_code == 200:
        data = response.text

        path: Path = context.op_config["path"]
        Path(path).parent.resolve().mkdir(parents=True, exist_ok=True)

        with path.open("w") as fd:
            fd.write(data)

        context.log.info(f"🚀 Fetch reports success.")
    else:
        context.log.info(
            f"🚀 Fetch reports error '{response.status_code}' - '{response.text}'..."
        )

    return path


@op(
    config_schema={
        "schema": Field(String),
    },
    ins={
        "path": In(String),
    },
    out={
        "zendesk_data": Out(),
        "has_more_data": Out(),
        "next_url": Out(),
        "count_reports": Out(),
        "output_message": Out(),
    },
)
def read_reports(
    context: OpExecutionContext, path: str
) -> tuple[pd.DataFrame, bool, str, int, str]:

    p = Path(path)
    with open(p, "r") as file:
        data = json.load(file)

    abautos_details_key_value = (
        17698062540823  # this indicates it is an abautos zendesk report
    )
    abautos_occupied_key_value = 14510509580823  # indicates it is the occupied field

    zendesk_id_list = []
    zendesk_value_list = []

    for report_record in data["results"]:

        details = ""
        zendeskID = " "
        vehicleColor = " "
        vehicleType = "UNKNOWN"
        vehicleMake = "UNKNOWN"
        vehicleLicense = "UNKNOWN"
        vehicleState = "UNKNOWN"
        vehicleDescription = ""
        vehicleArea = ""
        vehicleAddress = ""
        latitude = ""
        longitude = ""
        contactFirstName = ""
        contactLastName = ""
        contactPhone = ""
        contactEmail = ""
        waiveConfid = 0
        occupied = "UNKNOWN"

        zendesk_id = str(report_record["id"])

        ## the main data we are looking is on the value property
        ## of one of many custom field id/value pairs
        custom_fields = report_record["custom_fields"]

        ## filter custom fields to find the one for AbAutos Reports
        ## and return just the value pair for it as json
        abautos_report_fields = json.loads(
            [r for r in custom_fields if r["id"] == abautos_details_key_value][0][
                "value"
            ]
        )

        ## List of area assignments for officers
        areaAssigned = ["E", "N", "NE", "NW", "S", "SE", "SW", "W"]

        ## Get occupied field by looking for id=14510509580823 true means it is occupied
        find_occupied = ""
        find_occupied = [
            r for r in custom_fields if r["id"] == abautos_occupied_key_value
        ][0]["value"]
        find_occupied_record = [
            r for r in custom_fields if r["id"] == abautos_occupied_key_value
        ][0]["id"]
        occupied_caps = str(find_occupied).upper()
        if occupied_caps == "TRUE":
            occupied = "YES"
        else:
            if occupied_caps == "FALSE":
                occupied = "NO"
            else:
                occupied = "UNKNOWN"

        checkaddr = str(
            abautos_report_fields["report_location:location_address"]["value"]
        )

        ## Replace backslash and forward slash with blanks
        ## Note since slash is escape char , need two slashes
        checkaddr = checkaddr.replace("\\", "").replace("/", "")

        ## Need to get Area or Street Prefix
        location = checkaddr.split()
        i = 0
        found = 0
        while i < len(location) and found == 0:
            ##Loop location to get area
            checkarea = location[i].upper()
            ##check if it is an area
            for area in areaAssigned:
                if checkarea == area:
                    areaFound = checkarea

                    found = 1
                    # print("Found area: " + checkarea)
                    break
            i = i + 1
        if found == 0:
            ##Assigned to SE as default
            areaFound = "SE"
            # print("Not found area: " + areaFound + " Address: " + checkaddr)
        vehicleArea = areaFound
        vehicleAddress = checkaddr

        latitude = abautos_report_fields["report_location:location_lat"]["value"]
        longitude = abautos_report_fields["report_location:location_lon"]["value"]

        vehicleColor = abautos_report_fields["report_vehicle:color"]["value"].upper()
        vehicleMake = abautos_report_fields["report_vehicle:make"]["value"].upper()
        vehicleType = abautos_report_fields["report_vehicle:type"]["value"].upper()

        if "report_vehicle:license_plate_state" in abautos_report_fields:
            vehicleState = abautos_report_fields["report_vehicle:license_plate_state"][
                "value"
            ].upper()
        if "report_vehicle:license_plate_number" in abautos_report_fields:
            vehicleLicense = abautos_report_fields[
                "report_vehicle:license_plate_number"
            ]["value"].upper()

        if "contact_name" in abautos_report_fields:
            names = str(abautos_report_fields["contact_name"]["value"]).split()

            nameCount = len(names)
            if nameCount >= 2:
                contactFirstName = names[0]
                contactLastName = names[1]
            else:
                contactFirstName = abautos_report_fields["contact_name"]["value"]
                contactLastName = ""

        if "contact_phone" in abautos_report_fields:
            contactPhone = abautos_report_fields["contact_phone"]["value"]
        if "contact_email" in abautos_report_fields:
            contactEmail = abautos_report_fields["contact_email"]["value"]

        # Need to check if exists to take care of KeyError
        if "confidentiality_waiver" in abautos_report_fields:
            waived = abautos_report_fields["confidentiality_waiver"]["value"]
            if "I do not waive confidentiality" in waived:
                waiveConfid = 0
            else:
                if "I choose to waive confidentiality" in waived:
                    waiveConfid = 1
                else:
                    ## default is DO NOT WAIVE
                    waiveConfid = 0
        else:
            waiveConfid = 0

        # Details consist of the following fields
        # Need to check if exists to take care of KeyError
        # for all the keys below.
        if "report_is_camp" in abautos_report_fields:
            camp = str(abautos_report_fields["report_is_camp"]["value"]).strip()
            if len(camp) > 0:
                details = details + "Camp:" + camp + " "
            # print("Camp: "+ camp)

        if "report_vehicle_inoperable" in abautos_report_fields:
            inoperables = (
                str(abautos_report_fields["report_vehicle_inoperable"]["value"])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )
            if len(inoperables) > 0:
                details = details + inoperables + " "
            # print("Inoperables: "+ inoperables)

        if "report_location_is_private" in abautos_report_fields:
            isprivate = abautos_report_fields["report_location_is_private"][
                "value"
            ].strip()
            if len(isprivate) > 0:
                details = details + "Private:" + isprivate + " "
            # print("Isprivate : "+ isprivate )

        if "report_location:location_details" in abautos_report_fields:
            locdetails = (
                str(abautos_report_fields["report_location:location_details"]["value"])
                .strip()
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )
            if len(locdetails) > 0:
                details = details + "Private:" + locdetails + " "
            # print("Locdetails : "+ locdetails )

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
            # print("Locattr  : "+ locattr   )

        # Need to truncate details string because the stored procedure fails if max string length is > 128 char
        max_size = 128
        if len(details) <= max_size:
            vehicleDescription = details
        else:
            vehicleDescription = details[: (max_size - 1) - 3] + "..."

        create_case = (
            ","
            + "'"
            + vehicleColor
            + "'"
            + ","
            + "'"
            + vehicleType
            + "'"
            + ","
            + "'"
            + vehicleMake
            + "'"
            + ","
            + "'"
            + vehicleLicense
            + "'"
            + ","
            + "'"
            + vehicleState
            + "'"
            + ","
            + "'"
            + vehicleDescription
            + "'"
            + ","
            + "'"
            + vehicleArea
            + "'"
            + ","
            + "'"
            + vehicleAddress
            + "'"
            + ","
            + latitude
            + ","
            + longitude
            + ","
            + "'"
            + contactFirstName
            + "'"
            + ","
            + "'"
            + contactLastName
            + "'"
            + ","
            + "'"
            + contactPhone
            + "'"
            + ","
            + "'"
            + contactEmail
            + "'"
            + ","
            + str(waiveConfid)
            + ","
            + "'"
            + occupied
            + "'"
        )

        zendesk_id_list.append(zendesk_id)
        zendesk_value_list.append(create_case)

    meta_has_more = data["meta"]["has_more"].lower()

    if "false" in meta_has_more:
        has_more_data = bool(0)
    elif "true" in meta_has_more:
        has_more_data = bool(1)
    else:
        has_more_data = bool(1)

    next_url = data["links"]["next"]

    data = {"zendesk_id": zendesk_id_list, "attributes": zendesk_value_list}
    df = pd.DataFrame(data)
    count_reports = len(df.index)

    return (
        df,
        has_more_data,
        next_url,
        count_reports,
        "Read returns "
        + count_reports
        + " reports. Has more data is "
        + has_more_data
        + ".",
    )


@op(
    ins={
        "zendesk_data": In(pd.DataFrame),
    },
    out={"output_message": Out(str)},
)
def write_reports(context: OpExecutionContext, zendesk_data: pd.DataFrame):

    try:
        cnxn = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=PBOTSQLDEV2;"
            "Database=AbAutosMVC;"
            "Trusted_Connection=yes;"
        )
        ##IMPORTANT: In python, autocommit is off by default,
        ##so you have to set it to True like below.
        ##You do not set it in the pyodbc.connect statement above.
        ##It turns it off.
        cnxn.autocommit = True

        cursor = cnxn.cursor()

        for index, row in zendesk_data.iterrows():
            print(row["zendesk_id"], row["attributes"])
            # queryCaseExists ="""Exec sp_CaseExistsByZendeskID 187978"""
            queryCaseExists = "Exec sp_CaseExistsByZendeskID " + row["zendesk_id"]
            cursor.execute(queryCaseExists)
            for results in cursor.fetchall():
                if results[0] == 0:
                    # print("Zendesk Abandoned Autos case does not exists: "+ str(results[0]))
                    ##Create new case
                    # queryCreate ="""Exec sp_CreateAbCaseZendesk 187978,'UNKNOWN','4 DOOR','UNKNOWN','','OR','Private:Not sure Tax lot: R332503, Park id: 275 ','SE','MT TABOR PARK, PORTLAND  97215',45.5119281409899,-122.59159952402118,'PORTLAND','','','','joshua.gregor@portlandoregon.gov',0,'YES'"""
                    # """Exec sp_CreateAbCaseZendesk 185742,'ORANGE','GOLF CART','CADILLAC','','','Private:Not sure Private:Theres been a camp here since they finished building the "art" installation here several years ago. ','NE',' NE 102ND AVE-HALSEY ST RAMP, PORTLAND  97220',45.53452516452696,-122.55768030881883,'','','','gregoryscottclapp@gmail.com',0,'NO'"""
                    queryCreate = (
                        "Exec sp_CreateAbCaseZendesk "
                        + row["zendesk_id"]
                        + row["attributes"]
                    )
                    print(queryCreate)
                    cursor.execute(queryCreate)
                    for results in cursor.fetchall():
                        print(results)
                        if "Success" in results[1]:
                            context.log.info(
                                f"🚀 Write reports successfully with Abandoned Autos abcaseid: - '{str(results[0])}'."
                            )
                        else:
                            context.log.info(
                                f"🚀 Write reports failed for Zendesk ID: - '{row['zendesk_id']}'."
                            )
                else:
                    context.log.info(
                        f"🚀 Zendesk Abandoned Autos case already exists - abcaseid: - '{str(results[0])}'."
                    )

    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        sqlstate = sqlstate.split(".")
        print(sqlstate[-3])
        context.log.info(f"🚀 Write reports SQL error '{sqlstate[-3]}'.")
    finally:
        cnxn.close()


@job(
    resource_defs={
        "sql_server": mssql_resource,
        "io_manager": fs_io_manager,
    }
)
def process_zendesk_data(api_endpoint: str):
    has_more_data = True

    while has_more_data:
        path = fetch_reports(zendesk_api_endpoint=api_endpoint)
        read_reports(path=path)
        count_reports = read_reports["count_reports"]
        if count_reports > 0:
            write_reports(zendesk_data=read_reports["zendesk_data"])
        has_more_data = read_reports["has_more_data"]
        api_endpoint = read_reports["next_url"]
        remove_file(path)


@schedule(
    job=process_zendesk_data,
    cron_schedule="0 /5 * * *",
    execution_timezone="US/Pacific",
)
def zendesk_api_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time.isoformat()

    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "sql_server": {
                    "config": {"mssql_server_conn_id": "mssql_server_assets"}
                },
            },
            "ops": {
                "process_zendesk_data": {
                    "inputs": {
                        "api_endpoint": "https://portlandoregon.zendesk.com/api/v2/search/export?page[size]=1&filter[type]=ticket&query=group_id:18716157058327&ticket_form_id:17751920813847&created>"
                        + "${execution_date}"
                    },
                },
            },
        },
    )
