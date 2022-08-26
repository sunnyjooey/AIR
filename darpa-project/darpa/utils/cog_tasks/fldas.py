import os
from hashlib import md5
from pathlib import PurePosixPath
from requests import Request, Session
from urllib.parse import urlparse

# import pandas as pd
import rasterio as rio

from kiluigi import Task, IntermediateTarget, FinalTarget

# from luigi import WrapperTask
from luigi.parameter import ListParameter, DictParameter, Parameter
from luigi.format import Nop

CHUNK_SIZE = 2 ** 13


class DownloadFile(Task):
    prefix = Parameter()
    url = Parameter()

    _session = None

    @property
    def session(self):
        if not self._session:
            s = Session()
            s.auth = (
                os.environ["LUIGI_EARTHDATA_USERNAME"],
                os.environ["LUIGI_EARTHDATA_PASSWORD"],
            )
            with s.get(self.url) as r:
                self.url = r.url
            self._session = s
        return self._session

    def output(self):
        # ensure session is prepared, b/c it may change the url
        self.session
        url = urlparse(self.url)
        query = md5(url.query.encode())  # noqa: S303 - MD5 is fine for checksums
        path = query.hexdigest() / PurePosixPath(url.path).relative_to(
            "/"
        )  # .with_suffix(".tif")
        return IntermediateTarget(path=str(self.prefix / path), format=Nop)

    def run(self):
        with self.session.get(self.url, stream=True) as r:
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
        self.params = {
            "FORMAT": "Y29nLw",
            "SHORTNAME": "FLDAS_NOAH01_C_GL_MA",
            "SERVICE": "L34RS_LDAS",
            "VERSION": "1.02",
            "DATASET_VERSION": "001",
        }

        self.prefix = "models/poverty_model/data/geo_data/FLDAS/{Y}/{m}"

        self.param_update()

    def param_update(self):
        year, month = self.interval.split("-")
        self.prefix = self.prefix.format(Y=year, m=month)
        filename = (
            "/data/FLDAS/FLDAS_NOAH01_C_GL_MA.001/{Y}"
            "/FLDAS_NOAH01_C_GL_MA.ANOM{Y}{m}.001.nc"
        )
        label = "FLDAS_NOAH01_C_GL_MA.ANOM{Y}{m}.001.nc.SUB.tif"
        self.params.update(
            {
                "FILENAME": filename.format(Y=year, m=month),
                "LABEL": label.format(Y=year, m=month),
                "BBOX": "{1},{0},{3},{2}".format(*self.location["bbox"]),
            }
        )

    def output(self):
        p = PurePosixPath(self.prefix, self.params["LABEL"])
        return FinalTarget(path=str(p), format=Nop)

    def requires(self):
        url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/" "OTF/HTTP_services.cgi"
        tasks = []
        for variable in self.variables:
            self.params["VARIABLES"] = variable
            r = Request("GET", url=url, params=self.params).prepare()
            tasks.append(DownloadFile(prefix=self.prefix, url=r.url))
        return tasks

    def run(self):
        # get metadata
        targets = self.input()
        with rio.open(targets[0].path) as src:
            profile = src.profile
        dst_profile = profile.copy()
        dst_profile["count"] = len(targets)

        # combine into one COG
        with self.output().open("w") as f:
            with rio.open(f.name, "w", **dst_profile) as dst:
                for band, target in enumerate(targets, 1):
                    with rio.open(target.path) as src:
                        assert profile == src.profile
                        dst.write(src.read(1), band)
                        dst.update_tags(band, **src.tags(1))
