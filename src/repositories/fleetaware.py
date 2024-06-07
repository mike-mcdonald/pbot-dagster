import os

from datetime import datetime, timedelta, timezone

import requests

from msal import (
    ConfidentialClientApplication,
)

from dagster import (
    Array,
    EnvVar,
    Failure,
    Field,
    In,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Permissive,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SkipReason,
    String,
    fs_io_manager,
    job,
    op,
    repository,
    schedule,
    sensor,
)

from resources.fs import FileShareResource, fileshare_resource


def retrieve_token(
    tenant_id: str, client_id: str, client_secret: str, scopes: list[str]
):
    import truststore

    truststore.inject_into_ssl()

    app = ConfidentialClientApplication(
        client_id=client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )

    res = app.acquire_token_for_client(scopes=scopes)

    if "access_token" not in res:
        raise Exception("Received empty access token in response!")

    return res["access_token"]


@op(
    config_schema={
        "tenant_id": Field(
            String,
            description="Azure tenant ID",
        ),
        "client_id": Field(
            String,
            description="Azure client ID",
        ),
        "client_secret": Field(
            String,
            description="Client secret for Azure client",
        ),
        "scopes": Field(Array(String), description="Scopes to request a token for"),
    },
    out=Out(str),
)
def get_token(context: OpExecutionContext):
    return retrieve_token(
        context.op_config["tenant_id"],
        context.op_config["client_id"],
        context.op_config["client_secret"],
        context.op_config["scopes"],
    )


@op(
    config_schema={
        "email_address": Field(
            String,
            description="User email for extraction",
        ),
        "params": Field(
            Permissive(),
            description="Dictionary of parameters to pass to the list messages query.",
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

    context.run_id

    res = requests.get(
        f"https://graph.microsoft.com/v1.0/users/{context.op_config['email_address']}/mailFolders/inbox/messages",
        headers={"Authorization": f"Bearer {token}"},
        params=context.op_config["params"],
    )

    if res.status_code != 200:
        raise Failure(
            description=f"Received error code {res.status_code}",
            metadata={"response": MetadataValue.json(res.json())},
        )

    emails = res.json()["value"]

    if len(emails) == 0:
        raise Failure(
            description="Retrieved no emails!",
        )

    return [e["id"] for e in emails]


@op(
    required_resource_keys={"fs_destination"},
    config_schema={
        "email_address": Field(
            String,
            description="User email for extraction",
        ),
    },
    ins={
        "token": In(str),
        "emails": In(List, description="List of message IDs"),
    },
)
def download_attachments(context: OpExecutionContext, token: str, emails: list):
    import truststore

    truststore.inject_into_ssl()

    share: FileShareResource = context.resources.fs_destination

    headers = {"Authorization": f"Bearer {token}"}

    for id in emails:
        res = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{context.op_config['email_address']}/messages/{id}/attachments",
            headers=headers,
        )

        for attachment in res.json()["value"]:
            name = attachment["name"]
            content = requests.get(
                f"https://graph.microsoft.com/v1.0/users/{context.op_config['email_address']}/messages/{id}/attachments/{attachment['id']}/$value",
                headers=headers,
            )

            with open(os.path.join(share.client.host, name), "wb") as f:
                f.write(content.content)


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "fs_destination": fileshare_resource,
    }
)
def process_fleetaware_emails():
    token = get_token()
    download_attachments(token, get_emails(token))


@sensor(job=process_fleetaware_emails, minimum_interval_seconds=(60 * 5))
def fleetaware_sensor(context: SensorEvaluationContext):
    import truststore

    truststore.inject_into_ssl()

    token = retrieve_token(
        os.getenv("AZURE_TENANT_ID"),
        os.getenv("FLEETAWARE_APP_ID"),
        os.getenv("FLEETAWARE_APP_SECRET"),
        ["https://graph.microsoft.com/.default"],
    )

    dt = (
        datetime.fromtimestamp(context.last_completion_time)
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    res = requests.get(
        f"https://graph.microsoft.com/v1.0/users/{os.getenv('FLEETAWARE_USER')}/mailFolders/inbox/messages",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "$filter": f"receivedDateTime ge {dt} and subject eq 'PBOT KERBY GARAGE STATUS FOR EXPORT' and hasAttachments eq true",
        },
    )

    res.raise_for_status()

    emails = res.json()["value"]

    if len(emails) == 0:
        return SkipReason("No FleetAware export emails found")

    return RunRequest(
        run_key=dt,
        run_config={
            "resources": {
                "fs_destination": {
                    "config": {
                        "conn_id": "fs_fleetaware_exports",
                    }
                },
            },
            "ops": {
                "get_token": {
                    "config": {
                        "tenant_id": EnvVar("AZURE_TENANT_ID").get_value(),
                        "client_id": EnvVar("FLEETAWARE_APP_ID").get_value(),
                        "client_secret": EnvVar("FLEETAWARE_APP_SECRET").get_value(),
                        "scopes": ["https://graph.microsoft.com/.default"],
                    },
                },
                "get_emails": {
                    "config": {
                        "email_address": EnvVar("FLEETAWARE_USER").get_value(),
                        "params": {
                            "$select": "receivedDateTime, hasAttachments, subject",
                            "$filter": f"receivedDateTime ge {dt} and subject eq 'PBOT KERBY GARAGE STATUS FOR EXPORT' and hasAttachments eq true",
                            "$orderby": "receivedDateTime DESC",
                            "$top": 1,
                        },
                    },
                },
                "download_attachments": {
                    "config": {
                        "email_address": EnvVar("FLEETAWARE_USER").get_value(),
                    },
                },
            },
        },
    )


@repository
def fleetaware_repository():
    return [process_fleetaware_emails, fleetaware_sensor]
