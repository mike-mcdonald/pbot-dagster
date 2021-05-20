
from datetime import datetime, time
from dagster import pipeline, solid, resource, hourly_schedule, repository, ModeDefinition
# pip install dagster-azure, azure-core
from dagster_azure.adls2 import adls2_resource


@solid(required_resource_keys={"adls2"})
def list_containers(context):
    for x in context.resources.adls2.adls2_client.list_file_systems():
        context.log.info(str(x.name))


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={'adls2': adls2_resource}
        )
    ]
)
def azure_pipeline():
    list_containers()

# env variable for azure storage key AZURE_DATA_LAKE_STORAGE_KEY


@hourly_schedule(
    pipeline_name="azure_pipeline",
    start_date=datetime(2021, 5, 10),
    execution_time=time(hour=13, minute=43),
    execution_timezone="US/Pacific",
)
def azure_schedule(date):
    return {
        "resources": {
            "adls2": {
                "config": {
                    "storage_account": "pudldevtest",
                    "credential": {
                        "key": {
                            "env": "AZURE_DATA_LAKE_STORAGE_KEY"
                        }
                    }
                }
            }
        }
    }


@repository
def azure_repo():
    return [azure_pipeline, azure_schedule]
