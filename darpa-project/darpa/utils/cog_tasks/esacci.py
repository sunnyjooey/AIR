import os

# import re
# from pathlib import Path
from urllib.parse import urlparse
from pathlib import PurePosixPath

import rasterio
import rasterio.shutil

# from luigi import WrapperTask
from luigi.format import Nop
from luigi.parameter import Parameter, ListParameter, DictParameter

# from luigi.util import requires
# from requests import Session
import requests

from kiluigi import FinalTarget, Task, IntermediateTarget


CHUNK_SIZE = 2 ** 13


class DownloadFile(Task):
    prefix = Parameter()
    data = {
        "login": os.environ["LUIGI_CKAN_USERNAME"],
        "password": os.environ["LUIGI_CKAN_PASSWORD"],
    }
    resource = {
        "2017": "a006352b-a86b-47bf-9ae2-ba99d1ae989f",
        "2016": "b2c96a41-5b89-4cf2-b7e2-1f4d065de063",
        "2015": "3903ba98-6f35-41b0-98dc-570f7fa1a4bf",
        "2014": "2fb36970-b02b-471d-bfc4-67e4ba95eac9",
        "2013": "6088067f-3458-45c6-b8b3-8909aa0a25c0",
        "2012": "bae075af-6d9e-4211-b03c-e6baf0719270",
        "2011": "0e805c6a-e000-4e51-9304-81b6358b7a2e",
    }

    _session = None
    _url_download = None

    @property
    def session(self):
        if not self._session:
            # set session
            self._session = requests.Session()
            # set download url
            pref = self.prefix.split("/")[-1]
            self._url_download = "https://data.kimetrica.com/dataset/16a8f0b8-d1ff-4601-90a3-d96b80559374/resource/{}/download/".format(
                self.resource[pref]
            )
        return self._session

    def output(self):
        self.session
        path = PurePosixPath(urlparse(self._url_download).path).relative_to("/")
        return IntermediateTarget(path=str(self.prefix / path), format=Nop)

    def run(self):
        with self.session.get(self._url_download, headers=self.data, stream=True) as r:
            with self.output().open("w") as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)


class MakeCOG(Task):
    """
    merge the COG exported from GESDISC for each variable
    """

    interval = Parameter()
    variables = ListParameter()
    location = DictParameter()

    def __init__(self, *args):
        super().__init__(*args)
        self.prefix = "models/poverty_model/data/geo_data/ESACCI/{}"
        self.param_update()

    def param_update(self):
        self.prefix = self.prefix.format(self.interval)

    def output(self):
        p = PurePosixPath(self.prefix, "ESACCI_{}_WARPED.tif".format(self.interval))
        return FinalTarget(path=str(p), format=Nop)

    def requires(self):
        return DownloadFile(prefix=self.prefix)

    def run(self):
        with rasterio.open(self.input().path) as src, self.output().open("w") as dst:
            rasterio.shutil.copy(
                src,
                dst.name,
                driver="GTiff",
                TILED="YES",
                COPY_SRC_OVERVIEWS="YES",
                COMPRESS="DEFLATE",
            )
