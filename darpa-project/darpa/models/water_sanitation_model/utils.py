import datetime

import gdal
import numpy as np
import pandas as pd
import requests


def get_src_file(base_file, index, variable=None):
    ds = gdal.Open(base_file)
    src_file = ds.GetSubDatasets()[index][0]
    if variable:
        assert variable in src_file
    ds = None
    return src_file


def read_data(src_file):
    if src_file.endswith(".csv"):
        df = pd.read_csv(src_file)
    elif src_file.endswith((".DTA", ".dta")):
        try:
            df = pd.read_stata(src_file)
        except ValueError:
            df = pd.read_stata(src_file, convert_categoricals=False)
    else:
        raise NotImplementedError
    return df


def _start_day_of_ethiopian(year):
    """copied from: https://github.com/rgaudin/tools/blob/master/ethiopian_date/ethiopian_date/ethiopian_date.py#L37"""

    # magic formula gives start of year
    new_year_day = (year // 100) - (year // 400) - 4

    # if the prev ethiopian year is a leap year, new-year occrus on 12th
    if (year - 1) % 4 == 3:
        new_year_day += 1

    return new_year_day


def to_gregorian(year, month, date):
    """copied from: https://github.com/rgaudin/tools/blob/master/ethiopian_date/ethiopian_date/ethiopian_date.py#L75"""

    # prevent incorect input
    inputs = (year, month, date)
    if 0 in inputs or [data.__class__ for data in inputs].count(int) != 3:
        return np.nan
    else:
        # Ethiopian new year in Gregorian calendar
        new_year_day = _start_day_of_ethiopian(year)

        # September (Ethiopian) sees 7y difference
        gregorian_year = year + 7

        # Number of days in gregorian months
        # starting with September (index 1)
        # Index 0 is reserved for leap years switches.
        gregorian_months = [0, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30]

        # if next gregorian year is leap year, February has 29 days.
        next_year = gregorian_year + 1
        if (next_year % 4 == 0 and next_year % 100 != 0) or next_year % 400 == 0:
            gregorian_months[6] = 29

        # calculate number of days up to that date
        until = ((month - 1) * 30) + date
        if until <= 37 and year <= 1575:  # mysterious rule
            until += 28
            gregorian_months[0] = 31
        else:
            until += new_year_day - 1

        # if ethiopian year is leap year, paguemain has six days
        if year - 1 % 4 == 3:
            until += 1

        # calculate month and date incremently
        m = 0
        for i in range(0, gregorian_months.__len__()):
            if until <= gregorian_months[i]:
                m = i
                gregorian_date = until
                break
            else:
                m = i
                until -= gregorian_months[i]

        # if m > 4, we're already on next Gregorian year
        if m > 4:
            gregorian_year += 1

        # Gregorian months ordered according to Ethiopian
        order = [8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        gregorian_month = order[m]

        return datetime.date(gregorian_year, gregorian_month, gregorian_date)


class SessionWithHeaderRedirection(requests.Session):

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):

        super().__init__()

        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers

        url = prepared_request.url

        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)

            redirect_parsed = requests.utils.urlparse(url)

            if (
                (original_parsed.hostname != redirect_parsed.hostname)
                & (redirect_parsed.hostname != self.AUTH_HOST)
                & (original_parsed.hostname != self.AUTH_HOST)
            ):
                del headers["Authorization"]

        return
