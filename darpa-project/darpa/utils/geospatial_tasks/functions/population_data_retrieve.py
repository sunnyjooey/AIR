import re

import luigi
from luigi.configuration import get_config

from ckan_import import SS_CensusFilesCkan
from helper_func import retrieve_file
from ss_rates_payam import rates_mapping


def fetch_ckan_data(fname_out="census_rate_payam.csv"):
    """

    This function builds a luigi pipeline to fetch data from CKAN.
    For this to work, the config file needs to have the username, password and API key
    In the luigi.cfg file, also need to define directory path for cache_dir
    See ckan_import.py for more details on how to specify dataset to pullself.

    Parameters
    ----------
    fname_out : string
        Path and file name for output csv file.

    """

    config = get_config()
    logfile = config.get("core", "log_file_path")

    ## make sure cache_dir is set to the right directory, for example: /home/kimetrica/cache
    luigi.build([SS_CensusFilesCkan()], logging_conf_file=logfile)
    pop_data_dict = SS_CensusFilesCkan().output()

    ###
    ckan_output_ls = list(pop_data_dict)
    # regex to find the appropriate file types
    r_shp = re.compile(".*shapefile*")
    r_census = re.compile(".*census*")
    r_rates = re.compile(".*rate*")
    # declare path
    shp_path = pop_data_dict[list(filter(r_shp.match, ckan_output_ls))[0]].path
    census_path = pop_data_dict[list(filter(r_census.match, ckan_output_ls))[0]].path
    rates_path = pop_data_dict[list(filter(r_rates.match, ckan_output_ls))[0]].path

    # payam shape data
    payams_shp = retrieve_file(shp_path, ".shp")
    census_payam = retrieve_file(census_path, ".xlsx")
    rates_county = retrieve_file(rates_path, ".csv")
    ## fix census column headers to standard names in the rates file
    census_payam.rename(
        columns={
            "R_C": "R_Code",
            "S_C": "C_State",
            "C_C": "C_County",
            "PCode": "C_Payam",
            "Population": "SS2008",
        },
        inplace=True,
    )
    #### optional, get county population from census_payam using groupby

    ## merge census data with rates
    census_rate_payam = rates_mapping(census_payam, rates_county)
    payams_shp.to_file("data/SouthSudan_Payam_2008.shp", driver="ESRI Shapefile")
    census_rate_payam.to_csv(fname_out, index=False)
