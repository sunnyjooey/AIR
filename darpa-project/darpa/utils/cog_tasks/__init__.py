from luigi import WrapperTask

from . import esacci
from . import fldas
from . import landscan
from . import svdnb


class StockWarehouse(WrapperTask):
    def requires(self):
        yield esacci.MigrateToWarehouse()  # FIXME: prevent api call on .output()
        yield fldas.MigrateToWarehouse()
        yield landscan.MigrateToWarehouse()
        yield svdnb.MigrateToWarehouse()


# TODO: query-able catalog of the warehouse (e.g. stacspec.org)
