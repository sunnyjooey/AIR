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
        "2012-04": "703e1c6e-7d09-41cc-8f28-ce2e1c3afe75",
        "2012-05": "3d39d44d-36f7-448f-af6b-b119accf760b",
        "2012-06": "1d250aba-e282-42ac-a5e4-4db6ab1ae444",
        "2012-07": "6f228f4f-4f65-49c9-b47c-28333788b4b3",
        "2012-08": "0fa9b131-1171-4840-abda-e11a7717fc38",
        "2012-09": "0bfae77a-3017-4d0e-ac74-0b2c330ee3f1",
        "2012-10": "d06e795a-5bc1-47b5-87d5-8557d3cd8222",
        "2012-11": "44d334f5-fbf9-475d-a3bd-6b9c10c27a57",
        "2012-12": "9fdde9bc-9ca5-4bd2-833e-ef051f143e3f",
        "2013-01": "dac3933c-ac71-4209-8768-ed3c1118cb8e",
        "2013-02": "615b3139-0523-4847-8c08-1ad2eee6d820",
        "2013-03": "a95457ba-76eb-4b6b-a508-ffb602c5f959",
        "2013-04": "7cf5d5d5-d621-4140-8a64-e918ef0f46cf",
        "2013-05": "606c74e6-3f87-4fc6-923f-a3fcb5d84ecd",
        "2013-06": "c745716d-8dd6-45f5-a1a3-132e6079c2ec",
        "2013-07": "c2142eed-9575-41cb-b3ef-fdc069b86fc6",
        "2013-08": "968f0e72-4b34-44af-8b75-e6674ddc8dd2",
        "2013-09": "8b8281a5-cab8-4ff5-9841-0e6ca2b90e7d",
        "2013-10": "4246155d-6b28-49c4-9788-9d1189505c07",
        "2013-11": "f95d1511-dc44-4a91-8c50-357d6183af4f",
        "2013-12": "c2e47d29-564f-4350-88d9-bef7fa96bc52",
        "2014-01": "fc2d1b7a-56d5-478e-9c9e-3bbbd7bff73b",
        "2014-02": "8e36c9f3-c4f9-461f-b2ab-e7cee7883329",
        "2014-03": "bbdd821f-d13b-4931-848b-d241dd19aea0",
        "2014-04": "cb5cd481-5fcd-468d-b932-81ddccbb76dd",
        "2014-05": "dd7bd323-fc40-40c0-9d7e-2e253343179c",
        "2014-06": "252a608e-0931-4184-a011-0146b8468abc",
        "2014-07": "de95f73d-3c07-42e9-8952-c6afc5218e71",
        "2014-08": "bd0aa69f-8706-4af3-9831-7ffa22557eb0",
        "2014-09": "89d96c8a-194d-48f6-81e5-6bbd82b5c2cf",
        "2014-10": "b166b8f4-ce99-49b1-a3bc-75ef419409fd",
        "2014-11": "c8a2440a-5f70-4a64-b662-f4d5822f72a7",
        "2014-12": "95f71403-1737-414c-a10d-7f7d6d9c951a",
        "2015-01": "b3f20a35-0351-4ee1-a8d4-793e70910f1a",
        "2015-02": "5111ec65-9c28-42e8-9baf-c7fae758ad62",
        "2015-03": "61c64c19-fa89-45bd-8e8f-1ce667a37b94",
        "2015-04": "bc2bae56-8001-4a9d-810a-08a04a098f2b",
        "2015-05": "7acb4bc1-8e85-489c-8bde-4f1434113db8",
        "2015-06": "4415e964-034f-4e3e-938c-ef33716b3a79",
        "2015-07": "625d82f9-a29f-44aa-8844-781357fd2af5",
        "2015-08": "27d39354-3ab3-4f14-ab21-83345d9c3dd8",
        "2015-09": "5ad22cee-7726-4040-af92-0bf8d1953e0d",
        "2015-10": "74044204-c5ee-4d90-bf32-969b3e617d49",
        "2015-11": "794e3f3a-9ef5-47fe-8f7b-b90b2ab22b3e",
        "2015-12": "99990e61-0ae4-4110-bc47-83591ae9526d",
        "2016-01": "29429f3b-70d9-4dbd-8b0c-fad7d4d093d8",
        "2016-02": "fddf31ec-6147-47e2-985f-3407f2a61f4e",
        "2016-03": "251614ad-1607-4187-bfa0-ea6406f50de8",
        "2016-04": "a64dd6e9-faec-46b0-826b-037ab10a713a",
        "2016-05": "b2249df6-28fd-4f60-8d86-94de2cf20bed",
        "2016-06": "6820f3bb-8320-4be5-8385-04302949e385",
        "2016-07": "8040884e-729c-4aa8-b084-0cf4f11c0b15",
        "2016-08": "77c52e1e-114d-4d42-8873-6eafd127878a",
        "2016-09": "c29bc9a3-1cc9-4de2-a010-63832e56a30e",
        "2016-10": "7f7ea0c3-7480-4670-93ba-c4a85b36e0fa",
        "2016-11": "7ad2b3c7-e439-488b-8573-377eaa51e67c",
        "2016-12": "0558a1c9-e8a6-4887-907e-79766c9829a5",
        "2017-01": "f12c4a31-1f6b-4a99-87b8-341e11f4eb2a",
        "2017-02": "e382f34a-d55f-4312-855c-fb90a4549655",
        "2017-03": "80028941-a0d5-44df-af19-6ff79673421c",
        "2017-04": "cf54befa-6cbc-4620-a862-837491f682ce",
        "2017-05": "b95defd7-3c81-4392-9a95-30db7c4548fb",
        "2017-06": "2e58f669-7da1-4d06-b4bd-dfcd68d41bbc",
        "2017-07": "1d4242c8-44b0-4121-b2e4-fe15026a69e3",
        "2017-08": "64e30584-52b9-401a-9091-414a39cef535",
        "2017-09": "460939c4-c739-4e1b-bffd-e2cc027233e5",
        "2017-10": "cca61f42-fe40-4b9c-8f0b-79c018521f19",
        "2017-11": "4ca22744-c34d-4243-9f61-76d0764078d1",
        "2017-12": "39c3fd9f-de07-427e-9057-91d2f749349f",
    }

    _session = None
    _url_download = None

    @property
    def session(self):
        if not self._session:
            # set session
            self._session = requests.Session()
            # set download url
            pref = self.prefix.split("/")
            pref = "{}-{}".format(pref[-2], pref[-1])
            self._url_download = "https://data.kimetrica.com/dataset/19aa8884-ed48-4c25-88e9-c1f96d5877bc/resource/{}/download/".format(
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
        self.prefix = "models/poverty_model/data/geo_data/SVDNB/{Y}/{m}"
        self.param_update()

    def param_update(self):
        year, month = self.interval.split("-")
        self.prefix = self.prefix.format(Y=year, m=month)

    def output(self):
        p = PurePosixPath(self.prefix, "svdnb-{}.tif".format(self.interval))
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
