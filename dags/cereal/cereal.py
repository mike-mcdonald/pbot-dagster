import csv
from datetime import datetime, time

import requests
from dagster import repository  # repo.py
from dagster import (
    DagsterType,
    EventMetadataEntry,
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    TypeCheck,
    fs_io_manager,
    hourly_schedule,
    pipeline,
    solid,
)

"""
function to specify and verify the input/output data type of a dagster
def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(
        isinstance(element, dict) for element in value
    )
"""


def custom_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=(
                "SimpleDataFrame should be a list of dicts, got " "{type_}"
            ).format(type_=type(value)),
        )

    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    "SimpleDataFrame should be a list of dicts, "
                    "got {type_} for row {idx}"
                ).format(type_=type(row), idx=(i + 1)),
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=(
                    "Rows in SimpleDataFrame should have the same fields, "
                    "got {actual} for row {idx}, expected {expected}"
                ).format(actual=row_fields, idx=(i + 1), expected=fields),
            )

    return TypeCheck(
        success=True,
        description="SimpleDataFrame summary statistics",
        metadata_entries=[
            EventMetadataEntry.text(
                str(len(value)),
                "n_rows",
                "Number of rows seen in the data frame",
            ),
            EventMetadataEntry.text(
                str(len(value[0].keys()) if len(value) > 0 else 0),
                "n_cols",
                "Number of columns seen in the data frame",
            ),
            # see logs as events in DAG Run
            EventMetadataEntry.text(
                str(list(value[0].keys()) if len(value) > 0 else []),
                "column_names",
                "Keys of columns seen in the data frame",
            ),
        ],
    )


SimpleDataFrame = DagsterType(
    name="SimpleDF",
    type_check_fn=custom_data_frame_type_check,
    description="A naive representation of a DF, e.g. as returned by csv.DictReader ",
)

"""
specify the 'url' in config while running the dag as follows.
solids:
  get_cereal:
    config:
      url: "https://raw.githubusercontent.com/dagster-io/dagster/master/examples/docs_snippets/docs_snippets/intro_tutorial/cereal.csv"
"""


@solid(config_schema={"url": str}, output_defs=[OutputDefinition(SimpleDataFrame)])
def get_cereal(context):
    response = requests.get(context.solid_config["url"])
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@solid(input_defs=[InputDefinition("cereal", SimpleDataFrame)])
def cereal_with_most_sugar(_, cereal):
    sorted_by_sugar = sorted(cereal, key=lambda cereal: cereal["sugars"])
    return sorted_by_sugar[-1]["name"]


@solid(input_defs=[InputDefinition("cereal", SimpleDataFrame)])
def find_highest_calorie_cereal(_, cereal):
    sorted_cereals = list(sorted(cereal, key=lambda cereal: cereal["calories"]))
    return sorted_cereals[-1]["name"]


@solid(input_defs=[InputDefinition("cereal", SimpleDataFrame)])
def find_highest_protein_cereal(_, cereal):
    sorted_cereals = list(sorted(cereal, key=lambda cereal: cereal["protein"]))
    return sorted_cereals[-1]["name"]


@solid
def display_results(context, most_calories, most_protein, sugariest):
    date = context.solid_config["date"]
    context.log.info(f"Today is {date}. Most caloric cereal: {most_calories}")
    context.log.info(f"Today is {date}. Most protein-rich cereal: {most_protein}")
    context.log.info(f"Today is {date}. Sugariest cereal: {sugariest}")


# set higher priority number for important pipelines.

# tags={"dagster/priority": "3"} sets the priority of dag run based on tag.

# io_manager is set to fs_io_managet (persistency) to enable re-execution of a solid(task)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": fs_io_manager})])
def cereal_pipeline():
    cereals = get_cereal()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
        sugariest=cereal_with_most_sugar(cereals),
    )


@hourly_schedule(
    pipeline_name="cereal_pipeline",
    start_date=datetime(2021, 5, 19),
    execution_time=time(hour=6, minute=19),
    execution_timezone="US/Pacific",
)
def cereal_schedule(date):
    return {
        "solids": {
            "display_results": {"config": {"date": date.strftime("%Y-%m-%d")}},
            "get_cereal": {
                "config": {
                    "url": "https://raw.githubusercontent.com/dagster-io/dagster/master/examples/docs_snippets/docs_snippets/intro_tutorial/cereal.csv"
                }
            },
        }
    }


# repo.py


@repository
def dags_repo():
    return [cereal_pipeline, cereal_schedule]
