from string import Template
from typing import Any, Set

from dagster import OpExecutionContext


def create_mapping(
    context: Any,
    attribute_names: Set[str],
):
    mapping = dict()

    for prop in attribute_names:
        try:
            mapping[prop] = context.__getattribute__(prop)
        except Exception as err:
            context.log.warning(
                f"Failed to substitute '{prop}' from context: {err}", exc_info=err
            )
            continue

    return mapping


def apply_substitutions(
    template_string: str,
    substitutions: dict,
    context: OpExecutionContext,
):
    mapping = create_mapping(
        context, {"partition_key", "pipeline_name", "retry_number", "run_id"}
    )

    if substitutions:
        mapping.update(substitutions)

    return Template(template_string).safe_substitute(**mapping)
