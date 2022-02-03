from dagster import repository

from test.azure import test_azure_upload_files
from test.fs import test_list_dir_dynamic


@repository
def test_repository():
    return [test_azure_upload_files, test_list_dir_dynamic]
