import requests
import json
from os import path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

class LaunchLibraryOperator(BaseOperator):
    ui_color = '#555'
    ui_fgcolor = '#fff'
    template_fields = ('ds', 'tomorrow_ds')

    @apply_defaults
    def __init__(self, task_id, conn_id, endpoints, result_path, result_filename, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_id = task_id
        self.conn_id = conn_id
        self.endpoints = endpoints
        self.result_path = result_path
        self.result_filename = result_filename
        self.bucket = 'europe-west1-training-airfl-fdb83332-bucket'

    def execute(self, context, ds, tomorrow_ds):
        query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
        response = requests.get(query)
        print(f"responsewas {response}")
        remote_path = path.join(self.result_path, self.result_filename)
        upload_to_gcs(remote_path, self.bucket, self.result_filename)

    def upload_to_gcs(self, bucket, remote_path, response):
        gcs = GoogleCloudStorageHook()
        gcs.upload(bucket, remote_path, json.dumps(response),
                   mime_type='application/octet-stream')
