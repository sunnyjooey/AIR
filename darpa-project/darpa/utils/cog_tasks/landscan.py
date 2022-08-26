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
        "2017": "922b0c1e-1092-4e13-8a06-cd6db723af46",
        "2016": "1651b9ce-6cf2-41f0-b776-33c5fa9527b8",
        "2015": "8049856f-4746-46f5-abf6-267e62890b0a",
        "2014": "f9960989-3cfc-4dfd-844d-3ff68d1578ec",
        "2013": "a2113b9d-75bd-4a91-b1ee-3624a179c3a1",
        "2012": "7000487c-59d8-41cc-8347-0fcf212e70ac",
        "2011": "73cff4be-798f-4e6e-aed4-6b3b65070679",
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
            self._url_download = "https://data.kimetrica.com/dataset/ec2d7e53-d273-467e-9f12-dad6e97e25c9/resource/{}/download/".format(
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
        self.prefix = "models/poverty_model/data/geo_data/LANDSCAN/{}"
        self.param_update()

    def param_update(self):
        self.prefix = self.prefix.format(self.interval)

    def output(self):
        p = PurePosixPath(self.prefix, "LANDSCAN_{}_WARPED.tif".format(self.interval))
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
