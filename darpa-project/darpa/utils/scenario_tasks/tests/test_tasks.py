import datetime

import geopandas as gpd
from luigi.parameter import DateIntervalParameter
from shapely.geometry.polygon import Polygon

from utils.scenario_tasks.functions.time import MaskDataToTime
from utils.test_utils import LuigiTestCase

# TODO: Future tests may want to test filtering on vector objects other than polygons


class MaskDataToTimeTestCase(LuigiTestCase):
    """
    This test checks the functionality of MaskDataToTime functions
    """

    def setUp(self):
        super().setUp()
        self.test_dates = ["2018-01-01", "2018-01-01-2019-01-01"]
        self.start = datetime.date.fromisoformat("2018-01-01")
        self.end = datetime.date.fromisoformat("2019-01-01")
        self.date_in_range = datetime.date.fromisoformat("2018-06-01")
        self.date_out_range = datetime.date.fromisoformat("2019-06-01")
        self.mock_geodataframe_poly = gpd.GeoDataFrame(
            [
                [
                    "shape",
                    "2018-01-01T00:00:00",
                    2.5,
                    Polygon([[23, 12.5], [23, 2], [36, 2], [36, 12.5]]),
                ]
            ],
            columns=["name", "Time", "value", "geometry"],
        )

    def test_parse(self):
        """
        Check that DateIntervalParameter is properly parsing the Time.
        """
        time = DateIntervalParameter.parse(self.test_dates[0]).date_a
        assert type(time) is datetime.date

        time = [
            DateIntervalParameter.parse(self.test_dates[1]).date_a,
            DateIntervalParameter.parse(self.test_dates[1]).date_b,
        ]
        assert type(time[0]) is datetime.date
        assert type(time[1]) is datetime.date

    def test_apply_time_filter(self):
        """
        Check if the time filter is accurately identifying dates within the date range
        (if the DateIntervalParameter is an interval) or if the date matches a single
        date (if the DateIntervalParameter is a single date). Run checks for filtering
        dates within and outside of the desired time window.
        """
        time = DateIntervalParameter.parse(self.test_dates[0]).date_a

        # Check a time inside the time window
        check_in = MaskDataToTime(time).apply_time_filter(
            self.date_in_range, self.start, self.end
        )
        assert check_in is True

        # Check a time outside the time window
        check_out = MaskDataToTime(time).apply_time_filter(
            self.date_out_range, self.start, self.end
        )
        assert check_out is False

        # Check the start time
        check_start = MaskDataToTime(time).apply_time_filter(
            self.start, self.start, self.end
        )
        assert check_start is True

        # Check the end time
        check_end = MaskDataToTime(time).apply_time_filter(
            self.end, self.start, self.end
        )
        assert check_end is True

    def test_dataframe_time_filter(self):
        """
        Check if the dataframe filter function is retaining date that should fall within the time window.
        """
        # Convert geojson string format of Time into a datetime object
        self.mock_geodataframe_poly["Time"] = self.mock_geodataframe_poly["Time"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S")
        )
        data_filt = MaskDataToTime(self.test_dates[1]).dataframe_time_filter(
            self.mock_geodataframe_poly, self.start, self.end
        )
        assert data_filt.shape[0] == 1
