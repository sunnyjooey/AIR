from collections import OrderedDict

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features


def estimate_county_level_population(
    geoshape_file,
    pop_rates_file,
    ref_proj,
    fname_out="county_level_pop_out.tif",
    year=2017,
    no_data_val=-9999,
    birth_rate_fct=0.0,
    death_rate_fct=0.0,
    migration_fct=0.0,
):

    """ Estimate County level population based on latest census data and growth rate.

    This function is specific for South Sudan. The latest census was in 2008. This serves as
    the base estimation. For population estimation after 2008 a growth rate is applied which is
    given by the South Sudenese government.

    Parameters
    ----------
    geoshape_file : ESRI Shape file (*.shp)
        The geoshape_file has the county boundaries for South Sudan. These boundaries are used to
        rasterize the various population data (2008 census, CBR, CDR, and migration).

    pop_rates_file: CSV file (*.csv)
        This file has the population values by county (2008 census, CBR, CDR, and migration)

    ref_proj: Dictionary
        This is a dictionary containing the relavent reference projection information for the output
        raster file. This dictionary is composed from the get_raster_proj_config_file in
        geospatial tasks. For this function it should contain the following:

        'srs' : spatial reference system
        'ncolsrows'  : (number of columns, number of rows) size of output raster
        'pixel' : (originx, pixel width, 0, originy, 0, pixel height) Affine geotransform

    fname_out : string
        Path and file name for output raster.

    year : integer
        Year for population projection. Note for this specific case (South Sudan) it should be
        greater than 2008 (last census)

    no_data_val : float
        Value to assign no data points in output raster file.

    grwth_rate_fct : float
        This parameter is applied to the nominal growth rates. It is used for sensitivity studies
        of changes in the growth rate. Example, if one uses 0.1 this will boost the nominal
        growth rates by 10%.

    References
    ----------

    Notes
    -----

    """

    # Read the shapefile with total population data
    shp_data = gpd.read_file(geoshape_file)

    # Read the population data (csv file)
    pop_data = pd.read_csv(pop_rates_file)

    # Merge shape file and population data
    pop_data = pop_data.drop(["ADMIN2NAME", "ADMIN1NAME"], axis=1)
    pop_data = shp_data.merge(pop_data, on=["ADMIN1PCOD", "ADMIN2PCOD"], how="outer")

    # -------------------------------------
    # Calculate Population from 2008 Census
    # data and Crude Birth Rates (CBR) and
    # Crude Death Rates (CDR)
    # -------------------------------------
    for y_ind in range(2009, year + 1):
        prv_yr = str(y_ind - 1)
        y_str = str(y_ind)
        pop_data["SS" + y_str] = (
            pop_data["SS" + prv_yr]
            + pop_data["SS" + prv_yr] * pop_data["CBR_" + y_str] * (1 + birth_rate_fct)
            - pop_data["SS" + prv_yr] * pop_data["CDR_" + y_str] * (1 + death_rate_fct)
            + pop_data["INMIGRATION_" + y_str]
            - pop_data["OUTMIGRATION_" + y_str]
        )

        pop_data["SS" + y_str] = pop_data["SS" + y_str].apply(lambda x: np.round(x))
        pop_data["SS" + y_str] = pop_data["SS" + y_str].astype("int32")

    pop_data["Population"] = pop_data["SS" + str(year)]

    # -----------------------------------------------------------------------
    # Rasterize Total population along with age and gender ratios
    # Total population and age & gender ratios should go into one raster file
    # -----------------------------------------------------------------------
    (ncols, nrows) = ref_proj["ncolsrows"]
    rast_meta = {
        "driver": "GTiff",
        "height": nrows,
        "width": ncols,
        "count": 1,
        "dtype": np.int32,
        "crs": ref_proj["srs"],
        "transform": ref_proj["pixel"],
        "nodata": no_data_val,
    }

    # Open raster file for writing
    with rasterio.open(fname_out, "w", **rast_meta) as out_raster:

        out_array = out_raster.read(1)

        # Rasterize geopandas geometries with population values
        shapes = (
            (geom, pop_values)
            for geom, pop_values in zip(pop_data["geometry"], pop_data["Population"])
        )
        burned_data = features.rasterize(
            shapes=shapes, fill=0, out=out_array, transform=out_raster.transform
        )

        out_raster.write_band(1, burned_data)

        # Tag raster bands
        band_tags = {"band_1": "Total_Population"}
        out_raster.update_tags(**band_tags)


def create_county_cohorts(
    geoshape_file,
    pop_rates_file,
    age_gender_file,
    ref_proj,
    no_data_val=-9999,
    fname_out="county_level_cohort_out.tif",
    year=2017,
):

    """Rasterize county level cohorts.

    This functions rasterizes county level cohorts (ratios of age and gender) breakdown of total
    population. This is later applied to the total population raster to obtain raster files of
    various age and gender cohorts.

    Parameters
    ----------
    geoshape_file : ESRI Shape file (*.shp)
        The geoshape_file has the growth rate for each year by county. This data has been rasterized
        for each county in South Sudan. In addition, the shapefile contains the 2008 census County
        level population estimations. This file has been provided with the code. There should be no
        need to change this file.

    pop_rates_file: CSV file (*.csv)
        This file has the population values by county (2008 census, CBR, CDR, and migration)

    ref_proj: Dictionary
        This is a dictionary containing the relavent reference projection information for the output
        raster file. This dictionary is composed from the get_raster_proj_config_file in
        geospatial tasks. For this function it should contain the following:

        'srs' : spatial reference system
        'ncolsrows'  : (number of columns, number of rows) size of output raster
        'pixel' : (originx, pixel width, 0, originy, 0, pixel height) Affine geotransform

    no_data_val : float
        Value to assign no data points in output raster file.

    fname_out : string
        Path and file name for output raster.

    year : Integer
        Year for population cohort breakdown. Note this should correspond to the year of population
        estimation

    References
    ----------

    Notes
    -----

    """

    # Read the shapefile with total population data
    shp_data = gpd.read_file(geoshape_file)

    # Read the population data (csv file)
    pop_data = pd.read_csv(pop_rates_file)

    # Merge shape file and population data
    pop_data = pop_data.drop(["ADMIN2NAME", "ADMIN1NAME"], axis=1)
    pop_data = shp_data.merge(pop_data, on=["ADMIN1PCOD", "ADMIN2PCOD"], how="outer")

    # Read the csv file with age and gender cohort populations
    age_gender_data = pd.read_csv(age_gender_file)

    # Grab data for relevant year
    age_gender_data = age_gender_data.loc[age_gender_data["Year"] == year]
    age_gender_data.drop("Year", inplace=True, axis=1)

    # Normalize age and gender breakdown (Create fractions) according to total population
    total_pop = age_gender_data["Total"].iloc[0]
    age_gender_data["Total"] = age_gender_data["Total"] / total_pop
    age_gender_data["Male"] = age_gender_data["Male"] / total_pop
    age_gender_data["Female"] = age_gender_data["Female"] / total_pop

    # Create age and gender maps with the same areas defined in total population
    for row in age_gender_data.itertuples():
        pop_data[row.Age + "_Total"] = np.ones(pop_data.shape[0]) * row.Total

        pop_data[row.Age + "_Male"] = np.ones(pop_data.shape[0]) * row.Male

        pop_data[row.Age + "_Female"] = np.ones(pop_data.shape[0]) * row.Female

    # Rasterize Total population along with age and gender ratios
    # Total population and age & gender ratios should go into one raster file
    (ncols, nrows) = ref_proj["ncolsrows"]
    rast_meta = {
        "driver": "GTiff",
        "height": nrows,
        "width": ncols,
        "count": 65,
        "dtype": np.float32,
        "crs": ref_proj["srs"],
        "transform": ref_proj["pixel"],
        "nodata": no_data_val,
    }

    # Tags for raster bands
    clmn_names = [
        "0-4_Total",
        "5-9_Total",
        "10-14_Total",
        "15-19_Total",
        "20-24_Total",
        "25-29_Total",
        "30-34_Total",
        "35-39_Total",
        "40-44_Total",
        "45-49_Total",
        "50-54_Total",
        "55-59_Total",
        "60-64_Total",
        "65-69_Total",
        "70-74_Total",
        "75-79_Total",
        "80-84_Total",
        "85-89_Total",
        "90-94_Total",
        "95-99_Total",
        "100+_Total",
        "Total_Female",
        "0-4_Female",
        "5-9_Female",
        "10-14_Female",
        "15-19_Female",
        "20-24_Female",
        "25-29_Female",
        "30-34_Female",
        "35-39_Female",
        "40-44_Female",
        "45-49_Female",
        "50-54_Female",
        "55-59_Female",
        "60-64_Female",
        "65-69_Female",
        "70-74_Female",
        "75-79_Female",
        "80-84_Female",
        "85-89_Female",
        "90-94_Female",
        "95-99_Female",
        "100+_Female",
        "Total_Male",
        "0-4_Male",
        "5-9_Male",
        "10-14_Male",
        "15-19_Male",
        "20-24_Male",
        "25-29_Male",
        "30-34_Male",
        "35-39_Male",
        "40-44_Male",
        "45-49_Male",
        "50-54_Male",
        "55-59_Male",
        "60-64_Male",
        "65-69_Male",
        "70-74_Male",
        "75-79_Male",
        "80-84_Male",
        "85-89_Male",
        "90-94_Male",
        "95-99_Male",
        "100+_Male",
    ]

    # Open raster file for writing
    with rasterio.open(fname_out, "w", **rast_meta) as out_raster:

        out_array = out_raster.read(1)

        # Loop through population and age and gender ratios
        for i, clmn in enumerate(clmn_names):

            # Rasterize geopandas geometries with population values
            shapes = (
                (geom, pop_values)
                for geom, pop_values in zip(pop_data["geometry"], pop_data[clmn])
            )
            burned_data = features.rasterize(
                shapes=shapes, fill=0, out=out_array, transform=out_raster.transform
            )

            out_raster.write_band(i + 1, burned_data)

        # Tag raster bands
        band_tags = OrderedDict(
            {"band_" + str(i + 1): val for i, val in enumerate(clmn_names)}
        )
        out_raster.update_tags(**band_tags)
