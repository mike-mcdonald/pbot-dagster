import json

import os
import requests
import webbrowser

from dagster import (
    EnvVar,
    Field,
    In,
    List,
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

from datetime import datetime, timedelta, timezone
from msal import ConfidentialClientApplication, PublicClientApplication, SerializableTokenCache
from ops.template import apply_substitutions
from pathlib import Path


@op(
    config_schema={
        "fleetaware_app_id": Field(
            String,
            description="Application id from MS Asure registered app FLEETAWARE",
        ),
          "fleetaware_authority": Field(
            String,
            description="Authority used for MS auth call",
        ),
        "fleetaware_secretid": Field(
            String,
            description="Client secret id from MS Asure registered app FLEETAWARE",
        ),
          "fleetaware_objectid": Field(
            String,
            description="App Object ID",
        ),
    },
    out=Out(str),
)
def get_token(context: OpExecutionContext):

    import truststore
    truststore.inject_into_ssl()

    app_id= context.op_config["fleetaware_app_id"]
    auth = context.op_config["fleetaware_authority"]
    secret_id= context.op_config["fleetaware_secretid"]
    object_id= context.op_config["fleetaware_objectid"]

    config = {
        "authority": auth,
        "client_id": app_id,
        "client_secret": secret_id,
        "client_objectid": object_id,

    }

    app = ConfidentialClientApplication(
    client_id=app_id,
    authority=auth,
    client_credential=secret_id,
    )

    result = None
    if not result:
        result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
    )

    context.log.info(f"🛻 Access_token: {result['access_token']}")
    if "access_token" in result:
    # Get *this* application's application object from Microsoft Graph
        response = requests.get(
            f"https://graph.microsoft.com/v1.0/applications/{config['client_objectid']}",
        headers={"Authorization": f'Bearer {result["access_token"]}'},
         ).json()
        context.log.info(f" Graph API call result: {json.dumps(response, indent=2)}")
    else:
        context.log.info(f" Error encountered when requesting access token: " f"{result.get('error')}")
        context.log.info(f" result.get('error_description')")

    return(result["access_token"] or None)

@op(
    config_schema={
        "fleetaware_user": Field(
            String,
            description="User email for extraction",
        ),
        "msgraph_api_endpoint": Field(
            String,
            description="MS Graph API endpoint",
        ),
    },
     ins={
        "token": In(str),
    },
    out=Out(List),
)
def get_emails(context: OpExecutionContext, token: str):
    import truststore
    truststore.inject_into_ssl()
    user_email = context.op_config["fleetaware_user"]
    msgraph_api_endpoint = context.op_config["msgraph_api_endpoint"]
    context.log.info(f" Email: {user_email}")
    headers = {
    'Authorization': 'Bearer ' + token
    }

    # url=  f"{msgraph_api_endpoint}me/messages?$select=subject,sender&$filter=subject eq 'PBOT KERBY GARAGE STATUS FOR EXPORT'"
    url=  f"{msgraph_api_endpoint}users/{user_email}/messages?$select=subject,sender&$filter=subject eq 'PBOT KERBY GARAGE STATUS FOR EXPORT'"
    #?$search='+ 'subject:PBOT KERBY GARAGE STATUS FOR EXPORT "hasAttachments eq true"
    filter_query = f"date(createdDateTime) eq {(datetime.now(timezone.utc) - timedelta(hours=2)).strftime('%Y-%m-%d')} "

    params = {
        "top": 10, # max is 1000 messages per request
        "count": "true",
        "select": "createdDateTime, HasAttachments, subject",
        "filter": "subject eq 'PBOT KERBY GARAGE STATUS FOR EXPORT'",
        #"search": "subject eq PBOT",
        #"orderby": "createdDateTime DESC",

    }

    response = requests.get(msgraph_api_endpoint + 'users/' + user_email + '/mailFolders/inbox/messages/', headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.json())

    response_json = response.json()
    response_json.keys()

    response_json['@odata.count']
    emails = response_json['value']
    context.log.info( f" Emails: {emails}")
    return(emails)


@op(
    config_schema={
        "fleetaware_path": Field(
            String,
            description="Fleetaware data folder for emailattachments",
        ),
        "fleetaware_user": Field(
            String,
            description="User email for extraction",
        ),
        "msgraph_api_endpoint": Field(
            String,
            description="MS Graph API endpoint",
        ),
    },
    ins={
        "token": In(str),
        "emails": In(List),
    },
)
def download_attachments(context: OpExecutionContext, token: str, emails: List):
    import truststore
    truststore.inject_into_ssl()
    fleetaware_dir = context.op_config['fleetaware_path']
    user_email = context.op_config['fleetaware_user']
    msgraph_api_endpoint = context.op_config['msgraph_api_endpoint']
    headers = {
    'Authorization': 'Bearer ' + token
    }
    for email in emails:
        if email['hasAttachments']:
            email_id = email['id']
            download_email_attachments(email_id, headers, fleetaware_dir)


    def download_email_attachments(message_id, headers, fleetaware_dir):
        try:
            response = requests.get(
                msgraph_api_endpoint + 'users/' + user_email + '/messages/{0}/attachments'.format(message_id),
                headers=headers
            )

            attachment_items = response.json()['value']
            for attachment in attachment_items:
                file_name = attachment['name']
                attachment_id = attachment['id']
                attachment_content = requests.get(
                    msgraph_api_endpoint + 'users/' + user_email + '/messages/{0}/attachments/{1}/$value'.format(message_id, attachment_id)
                )
                context.log.info('Saving file {0}...'.format(file_name))
                with open(os.path.join(fleetaware_dir, file_name), 'wb') as _f:
                    _f.write(attachment_content.content)
            return True
        except Exception as e:
            context.log.info(e)
            return False


@job(
    resource_defs={
        "io_manager": fs_io_manager,
    }
)
def process_emails():
    token = get_token()
    download_attachments(token,get_emails(token))


@schedule(
    job=process_emails,
    cron_schedule="*/30 * * * *",
    execution_timezone="US/Pacific",
)
def fleetaware_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time
    execution_date = execution_date.isoformat()
    return RunRequest(
        run_key=execution_date,
        run_config={
            "ops": {
                 "get_token": {
                    "config": {
                        "fleetaware_app_id": EnvVar("FLEETAWARE_APP_ID").get_value(),
                        "fleetaware_authority": EnvVar("FLEETAWARE_AUTHORITY").get_value(),
                        "fleetaware_objectid": EnvVar("FLEETAWARE_APP_ID").get_value(),
                        "fleetaware_secretid": EnvVar("FLEETAWARE_APP_ID").get_value(),
                    },
                },
                "get_emails": {
                    "config": {
                        "fleetaware_user": EnvVar("FLEETAWARE_USER").get_value(),
                        "msgraph_api_endpoint": EnvVar("MSGRAPH_API_ENDPOINT").get_value(),
                    },
                },
                "download_attachments": {
                    "config": {
                        "fleetaware_path": f"//pbotdm2/FleetAware Maximo Data Export/Test/",
                        "fleetaware_user": EnvVar("FLEETAWARE_USER").get_value(),
                        "msgraph_api_endpoint": EnvVar("MSGRAPH_API_ENDPOINT").get_value(),
                    },
                },
            },
        }
    )

@repository
def fleetaware_repository():
    return [process_emails,fleetaware_schedule]
