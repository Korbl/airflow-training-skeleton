import requests
import json
import posixpath
import pathlib
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

    def execute(self, ds, tomrrow_ds, result_filename):
        query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
        result_path = f"/tmp/rocket_launches/ds={ds}"
        pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
        response = requests.get(query)
        print(f"responsewas {response}")
        upload_to_gcs(self.result_path, 'europe-west1-training-airfl-fdb83332-bucket', self.result_filename)

    def upload_to_gcs(local_path, bucket, destination):
        gcs = GoogleCloudStorageHook()
        files_and_dirs = os.listdir(local_path)
        files_to_upload = [file for file in files_and_dirs if isfile(join(local_path, file))]

        for file in files_to_upload:
            object = join(destination, file)
            gcs.upload(bucket, object, join(local_path, file), mime_type='application/octet-stream')
            # Then delete the file locally
            os.remove(join(local_path, file))
