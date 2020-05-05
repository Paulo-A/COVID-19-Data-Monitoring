from __future__ import print_function
import pandas as pd
from google.oauth2 import service_account
import requests
import io
from google.cloud import bigquery

def data_writer(data, project_name, dataset_name, table_name, credentials, write_type, schema):
    data.to_gbq(
        dataset_name + "." + table_name,
        project_name,
        chunksize = None,
        if_exists = write_type,
        table_schema = schema,
        credentials = service_account.Credentials.from_service_account_info(credentials)
    )
    return
