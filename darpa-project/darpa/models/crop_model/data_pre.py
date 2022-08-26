import datetime
import logging
import os
import urllib
import numpy as np

import luigi
import rasterio
import requests
from luigi import ExternalTask, Task
from luigi.util import requires

from kiluigi.targets import CkanTarget, IntermediateTarget

logger = logging.getLogger("luigi-interface")


class PullRasterFromCkan(ExternalTask):
    """Pull static rasters for crop model from CKAN
    """

    def output(self):
        return {
            "cellid": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "81dc3856-87ce-4323-9d21-be99b15cd65c"},
            )
        }


@requires(PullRasterFromCkan)
class ScrapeDataFromNASAPower(Task):
    """
    """

    start_date = luigi.DateParameter(default=datetime.date(1984, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date(2019, 7, 31))

    def output(self):
        return IntermediateTarget(f"{self.task_id}/", timeout=60 * 60 * 24 * 365)

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            cellid_path = self.input()["cellid"].path
            src = rasterio.open(cellid_path)
            cellid_array = src.read(1)

            rows, cols = np.where(cellid_array != src.nodata)
            index = 1
            for row, col in zip(rows, cols):
                logger.info(f"Getting data for point {index} of {len(rows)}")
                x, y = src.xy(row, col)
                cell_id = cellid_array[row, col]
                dst_file = os.path.join(tmpdir, f"{int(cell_id)}.WTH")
                self.get_data_from_power_nasa(
                    x, y, self.start_date, self.end_date, dst_file
                )
                index += 1

    def get_data_from_power_nasa(self, x, y, start_date, end_date, dst_file):
        """Scrape Maximum and minimum temperature, dewpoint temperature and relative humudity
        """
        base_url = "https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py?"

        parameter_map = {
            "request": "execute",
            "identifier": "SinglePoint",
            "parameters": "T2M_MIN",
            "startDate": start_date.strftime("%Y%m%d"),
            "endDate": end_date.strftime("%Y%m%d"),
            "lat": y,
            "lon": x,
            "userCommunity": "AG",
            "tempAverage": "DAILY",
            "outputList": "ICASA",
            "user": "anonymous",
        }

        url = base_url + urllib.parse.urlencode(parameter_map)
        logger.info(url)
        response = requests.get(url)
        response = response.json()
        assert "message" not in response, response["messages"][0]["Alert"][
            "Description"
        ]["Issue"]
        response = requests.get(response["outputs"]["icasa"])

        with open(dst_file, "wb") as fout:
            for chunk in response:
                fout.write(chunk)
