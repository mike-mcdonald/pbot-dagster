from dagster import fs_io_manager, job, RunConfig

from ops.fs import remove_file
from ops.sql_server import file_to_table, truncate_table
from repositories.assets.ops import (
    create_updates_factory,
    determine_eval_updates,
    extract_asset,
    insert_asset,
    update_asset,
    refresh_signfaces,
    signs_to_file,
)
from resources.mssql import mssql_resource


@job(
    resource_defs={
        "sql_server": mssql_resource,
        "io_manager": fs_io_manager,
    }
)
def sign_library_to_assets():
    path = file_to_table(signs_to_file(), truncate_table())
    remove_file(path)
    refresh_signfaces(start=path)


@job(
    resource_defs={
        "io_manager": fs_io_manager,
    }
)
def synchronize_bridge_fields():
    # extract bridge asset from PBOTGISDB
    pbot = extract_asset.alias("extract_pbot")()
    # extract bridges from CGIS GISDB1
    cgis = extract_asset.alias("extract_public")()

    # Create dataset of updates for CGIS GISDB1
    cgis_inserts = create_updates_factory(
        "create_cgis_inserts", "left_only", lambda df: df["Status"] == "ACTIVE"
    )(pbot, cgis)
    cgis_updates = create_updates_factory(
        "create_cgis_updates", "both", lambda df: df["Status"] == "ACTIVE"
    )(pbot, cgis)
    # Create dataset of updates for PBOTGISDB
    pbot_updates = create_updates_factory("create_pbot_updates", "both")(cgis, pbot)

    # # Write updates to PBOTGISDB
    insert_asset.alias("insert_cgis")(cgis_inserts)
    update_asset.alias("update_cgis")(cgis_updates)
    # # Write updates to CGIS GISDB1
    update_asset.alias("update_pbot")(pbot_updates)


@job
def update_bridge_evaluations():
    evals = extract_asset()

    # Create dataset of updates for CGIS GISDB1
    updates = determine_eval_updates(evals)

    # # Write updates to CGIS GISDB1
    update_asset(updates)
