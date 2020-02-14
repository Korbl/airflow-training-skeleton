import requests
import json
import posixpath
import pathlib
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LaunchLibraryOperator(BaseOperator):
    ui_color = '#555'
    ui_fgcolor = '#fff'
    template_fields = ('_myvar1',)

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, ds, tomrrow_ds, **kwargs):
        query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
        result_path = f"/tmp/rocket_launches/ds={ds}"
        pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
        response = requests.get(query)
        print(f"responsewas {response}")
        with open(posixpath.join(result_path, result_filename), "w") as f:
            print(f"Writingto file {f.result_filename}")
            f.write(response.text)
