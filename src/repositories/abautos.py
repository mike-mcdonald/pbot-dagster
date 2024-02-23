import json
import os
import numpy as np
import pandas as pd
import pyodbc
import re
import requests
import textwrap

from datetime import datetime
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
from pathlib import Path
from resources.mssql import mssql_resource


@op(
    config_schema={
        "zendesk_api_endpoint": Field(
            String,
            description="""Zendesk API endpoint to fetch reports.""",
        ),
        "scheduled_date": Field(
            String,
            description="""Job scheduled date in isoformat.""",
        ),
        "path": Field(
            String,
            description=""" Output.json location""",
        ),
    },
    out=Out(String, "The path to the output.json file created from calling the Zendesk API"),
)
def fetch_reports(context: OpExecutionContext):
    zendesk_api_endpoint = context.op_config["zendesk_api_endpoint"]
    scheduled_date = context.op_config["scheduled_date"]
   
    context.log.info(f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M')}: Fetch reports started for scheduled_date '{scheduled_date}'")
    context.log.info(f"🚀 Zendesk API endpoint: '{zendesk_api_endpoint}'")
    context.log.info(f"🚀 Scheduled_date: '{scheduled_date.strftime('%Y-%m-%d %H:%M')}'")

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
        path = context.op_config["path"]
        context.log.info(f"🚀 Path: '{path}'")
        with path.open("w") as fd:
            fd.write(data)

        context.log.info(f"🚀 Fetch reports success.")
    else:
        context.log.info(
            f"🚀 Fetch reports error '{response.status_code}' - '{response.text}'..."
        )

    return path

def area_searcher(search_str:str, search_list:str):
    search_obj = re.search(search_list, search_str)
    if search_obj :
        return_str = search_str[search_obj.start(): search_obj.end()]
    else:
        return_str = "SE"
    return return_str

@op(
    config_schema={
        "schema": Field(String),
        "zpath": Field(
            String,
            description=""" Dataframe parquet file location""",
        ),
    },
    ins={
        "path": In(String),
    },
    out=Out(
        String, 
        description="The Zendesk dataframe parquet file",
    ),
)
def read_reports(context: OpExecutionContext, path: str):
    p = Path(path)
    with open(p, "r") as file:
        data = json.load(file)

    abautos_details_key_value = (
        17698062540823  # this indicates it is an abautos zendesk report
    )
    abautos_occupied_key_value = 14510509580823  # indicates it is the occupied field
    area_list = [" E ", " N "," NE "," NW ", " S "," SE "," SW "," W "]
    areapattern = "|".join(area_list)

    column_name = ["Id"
        , "Color"
        , "Type"
        , "Make"
        , "State"
        , "License"
        , "Detail"
        , "Area" 
        , "Address"      
        , "Lat"
        , "Lng"
        , "FirstName"
        , "LastName"
        , "Phone"
        , "Email"
        , "Waived"
        , "Occupied"
        , "Names"
        ]

    id,vehColor,vehType,vehMake,vehState,vehLicense,detail,area,address,lat,lng, \
    firstName,lastName,phone,email,waived,occupied,names = [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]

    for report_record in data["results"]:
        zendeskId = str(report_record["id"])
        details = ""
        ## the main data we are looking is on the value property
        ## of one of many custom field id/value pairs
        custom_fields = report_record["custom_fields"]
        occupied_field = [r for r in custom_fields if r["id"] == abautos_occupied_key_value ][0]["value"]
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
        vehState.append(abautos_report_fields["report_vehicle:license_plate_state"]["value"].upper())
        vehLicense.append(abautos_report_fields["report_vehicle:license_plate_number"]["value"].upper())
        address.append(abautos_report_fields["report_location:location_address"]["value"]
            .replace(r"/","")
            .replace(r"\\",""))
        lat.append(abautos_report_fields["report_location:location_lat"]["value"])
        lng.append(abautos_report_fields["report_location:location_lon"]["value"])

        if "contact_name"  in abautos_report_fields:
            names.append(abautos_report_fields["contact_name"]["value"])
        if "contact_phone"  in abautos_report_fields: 
            phone.append(abautos_report_fields["contact_phone"]["value"])
        if "contact_email"  in abautos_report_fields: 
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

        #Need to truncate details string because the stored procedure fails if max string length is > 128 char
        max_size= 128
        if len(details) <= max_size:
            description = details
        else:
            description = textwrap.wrap(details, max_size-3)[0] + "..."
        detail.append(description)

    df = pd.DataFrame([id,vehColor,vehType,vehMake,vehState,vehLicense,detail,area,address,lat,lng,firstName,lastName,phone,email,waived,occupied,names]).T
    df.columns=column_name

    df["Area"] = df["Address"].apply(lambda x: area_searcher(search_str=x, search_list=areapattern))
    df["FirstName"] = df["Names"].astype(str).str.split().str[0]
    df["LastName"] = df["Names"].astype(str).str.split().str[1]
    df["Waived"] = (
        np.select(
            condlist = [
                (df["Waived"]=="I do not waive confidentiality"),
                (df["Waived"]=="I choose to waive confidentiality"),
                (df["Waived"]=="I waive confidentiality")
                ],
            choicelist = [0,1,1],
            default = 0
            )
        )

    df["Occupied"] = (
        np.select(
            condlist=[df["Occupied"] == True, df["Occupied"] == False],
            choicelist=["YES", "NO"], 
            default="UNKNOWN"))

    has_more = data["meta"]["has_more"]
    next_url = data["links"]["next"]
    count_reports = len(df.index)
    context.log.info(f"🚀{datetime.now().strftime('%Y-%m-%d %H:%M')}: Read {count_reports} Zendesk Abandoned Autos reports. Has more reports is {has_more} ")

    zpath = context.op_config["zpath"]
    df.to_parquet(zpath, index=False)
    return zpath

@op(
    ins={
        "zpath": In(String),
    },
    out={"output_message": Out(str)},
)
def write_reports(context: OpExecutionContext, zpath: str):
    # read path as Path
    p = Path(zpath)
    zendesk_data = pd.read_parquet(p)

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
            # queryCaseExists ="""Exec sp_CaseExistsByZendeskID 187978"""
            queryCaseExists = "Exec sp_CaseExistsByZendeskID "+ row["Id"]
            print(queryCaseExists)
            cursor.execute(queryCaseExists)
            for results in cursor.fetchall():
                if results[0] == 0:
                    # print("Zendesk Abandoned Autos case does not exists: "+ str(results[0]))
                    ##Create new case
                    queryCreate = ("Exec sp_CreateAbCaseZendesk " + row["Id"]
                    + ","
                    + "'"
                    + row["Color"] 
                    + "'"
                    + ","
                    + "'"
                    +  row["Type"] 
                    + "'"
                    + ","
                    + "'"
                    + row["Make"]
                    + "'"
                    + ","
                    + "'"
                    + row["License"]
                    + "'"
                    + ","
                    + "'"
                    + row["State"]
                    + "'"
                    + ","
                    + "'"
                    + row["Detail"] 
                    + "'"
                    + ","
                    + "'"
                    + row["Area"]
                    + "'"
                    + ","
                    + "'"
                    + row["Address"]
                    + "'"
                    + ","
                    + row["Lat"]
                    + ","
                    + row["Lng"]
                    + ","
                    + "'"
                    + row["FirstName"]
                    + "'"
                    + ","
                    + "'"
                    + row["LastName"]
                    + "'"
                    + ","
                    + "'"
                    + row["Phone"]
                    + "'"
                    + ","
                    + "'"
                    + row["Email"]
                    + "'"
                    + ","
                    +str(row["Waived"])
                    + ","
                    + "'"
                    + row["Occupied"]
                    + "'" )
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
                                f"🚀 Write reports failed for Zendesk ID: - '{row['Id']}'."
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
def process_zendesk_data():
    path = fetch_reports()
    write_reports(read_reports(path))
    remove_file(path)
    

@schedule(
    job=process_zendesk_data,
    cron_schedule="*/5 * * * *",
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
                "fetch_reports": {
                    "config": {
                        "zendesk_api_endpoint": "https://portlandoregon.zendesk.com/api/v2/search/export?page[size]=1000&filter[type]=ticket&query=group_id:18716157058327&ticket_form_id:17751920813847&created>${execution_date}",
                        "scheduled_date": "${execution_date}",
                        "path": "//pbotdm2/abautos/output.json",
                    },
                },
                "read_reports": {
                    "config": {
                        "schema": "AbautosMVC",
                        "zpath": "//pbotdm2/abautos/zendesk.parquet",
                    },
                    "inputs": { "path": "//pbotdm2/abautos/output.json" },
                },
                "write_reports": {
                    "inputs": { "zpath" : "//pbotdm2/abautos/zendesk.parquet" },
                },
            },
        },
    )
