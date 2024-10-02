from dagster import (
    fs_io_manager,
    job,
)

from ops.df.write import write_dataframe
from ops.sftp.list import list_dynamic
from ops.sftp.move import move
from resources.ssh import ssh_resource
from .ops.analyze_files import analyze_files
from .ops.combine_dataframe import combine_dataframes
from .ops.create_rename_dataframe import create_rename_dataframe
from .ops.download_files import download_files
from .ops.execute_renames import execute_renames
from .ops.get_file_dataframe import get_file_dataframe


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "ssh_client": ssh_resource,
    }
)
def analyze_driver_records():

    files = []
    files.append(get_file_dataframe.alias("get_lyft_files")())
    files.append(get_file_dataframe.alias("get_uber_files")())

    all_files = combine_dataframes.alias("combine_files")(files)

    analysis = []
    analysis.append(
        analyze_files.alias("analyze_lyft_bgc")(
            download_files.alias("download_lyft_bgc")(all_files)
        )
    )
    analysis.append(
        analyze_files.alias("analyze_lyft_mvr")(
            download_files.alias("download_lyft_mvr")(all_files)
        )
    )
    analysis.append(
        analyze_files.alias("analyze_uber_bgc")(
            download_files.alias("download_uber_bgc")(all_files)
        )
    )
    analysis.append(
        analyze_files.alias("analyze_uber_mvr")(
            download_files.alias("download_uber_mvr")(all_files)
        )
    )

    execute_renames(
        create_rename_dataframe(
            all_files, combine_dataframes.alias("combine_analysis")(analysis)
        )
    )

    lyft_csv = list_dynamic.alias("list_lyft_csv")().map(move.alias("move_lyft_csv"))
    uber_csv = list_dynamic.alias("list_uber_csv")().map(move.alias("move_uber_csv"))

    # results = []
    # results.append(analyze_bgcfiles(download_files.alias("download_bgc")(files)))
    # results.append(analyze_dmvfiles(download_files.alias("download_dmv")(dmv_files)))

    # all_files = rename_files(results)

    # uber_files = upload_files.alias("upload_uber")(
    #     write_csv.alias("write_uber")(get_files.alias("get_uber_files")(all_files))
    # )
    # get_other_files.alias("get_uber_other_file")(
    #     get_unique_ids.alias("get_unique_uber_id")(uber_files)
    # )
    # # move_files.alias("move_uber_other")( get_other_file.alias("get_uber_other_file")(get_unique_id.alias("get_unique_uber_id")(uber_files)))
    # lyft_files = upload_files.alias("upload_lyft")(
    #     write_csv.alias("write_lyft")(get_files.alias("get_lyft_files")(all_files))
    # )
    # get_other_files.alias("get_lyft_other_file")(
    #     get_unique_ids.alias("get_unique_lyft_id")(lyft_files)
    # )
    # # move_files.alias("move_lyft_other")(get_other_file.alias("get_lyft_other_file")(get_unique_id.alias("get_unique_lyft_id")(lyft_files)))

    # # delete_files.alias("delete_uber")(uber_files)
    # # delete_files.alias("delete_lyft")(lyft_files)
