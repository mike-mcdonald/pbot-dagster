from dagster import repository

from test.azure import test_azure_upload_files


@repository
def test_repository():
    return [test_azure_upload_files]
