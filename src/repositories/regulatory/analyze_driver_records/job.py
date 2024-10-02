from dagster import (
    fs_io_manager,
    job,
)

from ops.df.filter import filter_dataframe
from ops.df.write import write_dataframe
from ops.fs.remove import remove_dir_config
from ops.sftp.list import list_dynamic
from ops.sftp.move import move
from ops.sftp.put import put
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
        "sftp": ssh_resource,
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

    renames = create_rename_dataframe(
        all_files, combine_dataframes.alias("combine_analysis")(analysis)
    )

    remove_dir_config.alias("cleanup_dir")(
        [
            execute_renames(renames),
            put.alias("put_lyft_results")(
                write_dataframe.alias("write_lyft_results")(
                    filter_dataframe.alias("filter_lyft_results")(renames)
                )
            ),
            put.alias("put_uber_results")(
                write_dataframe.alias("write_uber_results")(
                    filter_dataframe.alias("filter_uber_results")(renames)
                )
            ),
        ]
    )

    list_dynamic.alias("list_lyft_csv")().map(move.alias("move_lyft_csv"))
    list_dynamic.alias("list_uber_csv")().map(move.alias("move_uber_csv"))
