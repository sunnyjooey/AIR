import os
import pickle

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from affine import Affine
from models.malnutrition_model.ckan_import_maln import get_files, get_indep_vars
from models.malnutrition_model.functions.helper_func import retrieve_file
from models.malnutrition_model.functions.maln_utility_func import (
    add_missing_dummy_columns,
    chirps_stack,
    county_expend,
    crop_df,
    malnutrition_mapping,
    mean_ndvi_stack,
)
from rasterio import features
from sklearn.externals import joblib

MODEL_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# default values for malnutrition_inference, to avoid calls in argument defaults
# (B008)
fname_out = os.path.join(MODEL_ROOT_DIR, "ss_malnut_inference.csv")
model_gbm_gam = os.path.join(MODEL_ROOT_DIR, "maln_gb_model(gam)_v2.sav")
model_svm_gam = os.path.join(MODEL_ROOT_DIR, "maln_svm_model(gam)_v2.sav")
model_gbm_sam = os.path.join(MODEL_ROOT_DIR, "maln_gb_model(sam)_v2.sav")
model_svm_sam = os.path.join(MODEL_ROOT_DIR, "maln_svm_model(sam)_v2.sav")
scaler = os.path.join(MODEL_ROOT_DIR, "maln_scaler_v2.save")


def malnutrition_inference(
    fname_in,
    year=2017,
    month="May",
    fname_out=fname_out,
    model_gbm_gam=model_gbm_gam,
    model_svm_gam=model_svm_gam,
    model_gbm_sam=model_gbm_sam,
    model_svm_sam=model_svm_sam,
    scaler=scaler,
):

    """ This function is specific for South Sudan.
        This function makes predictions on malnutration rate (GAM and SAM) and the output is in .csv format

        Parameters:
            year: this can be user specified when calling this function and within a Luigi workflow
        (default 2017)
            month: this can be user specified when calling this function and within a Luigi workflow
        (default 'May')
            fname_in: input csv file with county names (specific to South Sudan), this template will be poplated
                with other explanatory vars based on year and month
            fname_out: output file of the predicted GAM, SAM rate values
-----------
    """
    luigi.build([get_indep_vars(), get_files()])

    ckan_data_dict = get_indep_vars().output()
    ckan_output_ls = list(ckan_data_dict)
    ckan_shape_dict = get_files().output()
    ckan_shape_ls = list(ckan_shape_dict)

    # declare paths
    ndvi_dir = ckan_data_dict[ckan_output_ls[0]].path
    chirps_dir = ckan_data_dict[ckan_output_ls[1]].path
    cpi_dir = ckan_data_dict[ckan_output_ls[2]].path
    ss_rates_dir = ckan_data_dict[ckan_output_ls[3]].path
    crop_dir = ckan_data_dict[ckan_output_ls[4]].path
    expend_dir = ckan_data_dict[ckan_output_ls[5]].path
    county_dir = ckan_shape_dict[ckan_shape_ls[0]].path

    #### unzip data then import ########
    ndvi_path = os.path.join("ckan_data/", ndvi_dir.split("/")[-2]) + "/"
    chirps_path = os.path.join("ckan_data/", chirps_dir.split("/")[-2]) + "/"
    crop_path = os.path.join("ckan_data/", crop_dir.split("/")[-2]) + "/"
    county_path = os.path.join("ckan_data/", county_dir.split("/")[-2]) + "/"

    ############ process data ##############
    expend_df = county_expend(county_path, expend_dir)
    ndvi = mean_ndvi_stack(ndvi_path)
    chirps = chirps_stack(chirps_path, lag=3)
    cpi_df = retrieve_file(cpi_dir, ".csv")
    ss_rates = retrieve_file(ss_rates_dir, ".csv")
    crop_data = crop_df(crop_path)
    crop_data.rename(
        columns={"canal/pigi": "canal pigi", "luakpiny/nasir": "nasir"}, inplace=True
    )

    smart_df = pd.read_csv(fname_in)
    smart_df["Year"] = int(year)
    smart_df["Month"] = month
    smart_df["county_lower"] = smart_df["County"]
    smart_df["county_lower"].replace(
        {
            "Canal/Pigi": "Canal Pigi",
            "Luakpiny/Nasir": "Nasir",
            "Kajo-keji": "Kajo keji",
        },
        inplace=True,
    )

    # load the typo dictionary
    pkl_file_path = os.path.join(MODEL_ROOT_DIR, "typo_dict.pkl")
    pkl_file = open(pkl_file_path, "rb")
    typo_dictionary = pickle.load(pkl_file)  # noqa: S301 - ignore secuirty check
    pkl_file.close()
    typo_list = list(typo_dictionary.keys())

    rev_cname = []
    for _, row in smart_df.iterrows():
        if row["county_lower"] in typo_list:
            rev_cname.append(typo_dictionary[row["county_lower"]])
        else:
            rev_cname.append(row["county_lower"].strip())

    crop_cnames = list(crop_data.columns.unique())
    ndvi_cnames = list(ndvi["County"].unique())
    chirps_cnames = list(chirps["County"].unique())
    expend_cnames = list(expend_df["County"].unique())

    ########## populate the variable dataframe for model inference ##########
    pop = []
    production = []
    ndvi_val = []
    cpi_idx = []
    chirp_val = []
    med_exp = []
    month = []
    year = []
    # smart_df['county_lower']=rev_cname
    for _, row in smart_df.iterrows():
        month.append(row["Month"])
        year.append(row["Year"])
        c_name = row["county_lower"].lower()
        ## get population estimate and cereal produced in the year prior:
        if c_name in crop_cnames:
            yr = str(row["Year"])
            yr_last = str(row["Year"] - 1)
            r = " Mid-" + yr + " Population"
            t = yr_last + " Net Cereal production"
            for indx in crop_data.index:
                if r in indx:
                    pop_val = int(crop_data.loc[indx, c_name].replace(",", ""))
                    pop.append(pop_val)
                if t in indx:
                    crop_tonne = int(crop_data.loc[indx, c_name].replace(",", ""))
                    production.append(crop_tonne)
        else:
            # print('this is not in crop df: '+c_name)
            pop.append(np.nan)
            production.append(np.nan)

        if c_name in ndvi_cnames:
            val = ndvi[
                (ndvi["Month"] == row["Month"])
                & (ndvi["County"] == c_name)
                & (ndvi["Year"] == str(row["Year"]))
            ]["NDVI"].values[0]
            ndvi_val.append(val)
        else:
            # print ('this not in ndvi: '+ c_name)
            ndvi_val.append(np.nan)

        if c_name in chirps_cnames:
            chp_val = chirps[
                (chirps["County"] == c_name)
                & (chirps["Year"] == str(row["Year"]))
                & (chirps["Month"] == row["Month"])
            ]["CHIRPS(mm)_lag3"].values[0]
            chirp_val.append(chp_val)
        else:
            # print('this is not in chirps df: '+c_name)
            chirp_val.append(np.nan)

        if c_name in expend_cnames:
            expend_val = expend_df[expend_df["County"] == c_name]["med_expend"].values[
                0
            ]
            med_exp.append(expend_val)
        else:
            # print('this is not in median expenditure df: '+c_name)
            med_exp.append(np.nan)

        # gdp_val.append(gdp[gdp['Country Name']=='South Sudan'][str(row['Year'])].values[0])
        cpi_yr = cpi_df.index[cpi_df["Year"] == str(row["Year"])].tolist()[0]
        cpi_val = cpi_df[row["Month"]][cpi_yr]
        cpi_idx.append(int(cpi_val.replace(",", "")))

    smart_vars = pd.DataFrame(
        {
            "Population": pop,
            "Month": month,
            "Net crop production": production,
            "NDVI": ndvi_val,
            "Year": year,
            "med_exp": med_exp,
            "CPI": cpi_idx,
            "CHIRPS(mm)_lag3": chirp_val,
        }
    )

    smart_vars["crop_per_capita"] = (
        smart_vars["Net crop production"] / smart_vars["Population"]
    )

    month_dummy = pd.get_dummies(smart_vars.Month)
    month_dummy = add_missing_dummy_columns(month_dummy)
    smart_df2 = pd.concat([smart_vars, month_dummy], axis=1)
    select_vars = [
        "NDVI",
        "Population",
        "CPI",
        "crop_per_capita",
        "CHIRPS(mm)_lag3",
        "med_exp",
        "Apr",
        "Aug",
        "Dec",
        "Feb",
        "Jan",
        "Jul",
        "Jun",
        "Mar",
        "May",
        "Nov",
        "Oct",
        "Sep",
    ]
    select_df = smart_df2[select_vars].dropna()

    scaler = joblib.load(scaler)
    select_transform = scaler.fit_transform(select_df[select_vars[:6]])
    select_df[
        ["NDVI", "Population", "CPI", "crop_per_capita", "CHIRPS(mm)_lag3", "med_exp"]
    ] = select_transform

    gb_gam = joblib.load(model_gbm_gam)
    svm_gam = joblib.load(model_svm_gam)
    gb_sam = joblib.load(model_gbm_sam)
    svm_sam = joblib.load(model_svm_sam)

    predicted_gb_gam = gb_gam.predict(select_df[select_vars])
    predicted_svm_gam = svm_gam.predict(select_df[select_vars])
    ensemble_pred_gam = np.mean((predicted_svm_gam, predicted_gb_gam), axis=0)

    predicted_gb_sam = gb_sam.predict(select_df[select_vars])
    predicted_svm_sam = svm_sam.predict(select_df[select_vars])
    ensemble_pred_sam = np.mean((predicted_svm_sam, predicted_gb_sam), axis=0)

    select_df["gam_rate"] = ensemble_pred_gam
    select_df["sam_rate"] = ensemble_pred_sam
    select_df["County"] = smart_df["County"]
    census_malnutrition = malnutrition_mapping(ss_rates, select_df)

    census_malnutrition.to_csv(fname_out, index=False)


def estimate_county_level_malnutrition(
    geoshape_file,
    pop_rates_file,
    ref_proj,
    fname_out="county_level_malnutrition_out.tif",
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
    # only want malnutration data for input var 'year', malnutrition rate should be in decimal
    pop_data["malnutrition_GAM"] = (
        pop_data["Population"] * 0.16 * (pop_data["gam_rate"])
    ).astype("int32")
    pop_data["malnutrition_SAM"] = (
        pop_data["Population"] * 0.16 * (pop_data["sam_rate"])
    ).astype("int32")

    # -----------------------------------------------------------------------
    # Rasterize  population along with age and gender ratios
    # Total population and age & gender ratios should go into one raster file
    # -----------------------------------------------------------------------
    (ncols, nrows) = ref_proj["ncolsrows"]
    affine_array = Affine(
        ref_proj["pixel"][1],
        ref_proj["pixel"][2],
        ref_proj["pixel"][0],
        ref_proj["pixel"][4],
        ref_proj["pixel"][5],
        ref_proj["pixel"][3],
    )
    rast_meta = {
        "driver": "GTiff",
        "height": nrows,
        "width": ncols,
        "count": 2,
        "dtype": np.int32,
        "crs": ref_proj["srs"],
        "transform": affine_array,  # ref_proj['pixel'],
        "nodata": no_data_val,
    }

    # Open raster file for writing
    with rasterio.open(fname_out, "w", **rast_meta) as out_raster:

        # out_array1 = out_raster.read(1)
        # out_array2 = out_raster.read(2)

        # Rasterize geopandas geometries with malnutrition values
        shapes_gam = (
            (geom, gam_values)
            for geom, gam_values in zip(
                pop_data["geometry"], pop_data["malnutrition_GAM"]
            )
        )
        shapes_sam = (
            (geom, sam_values)
            for geom, sam_values in zip(
                pop_data["geometry"], pop_data["malnutrition_SAM"]
            )
        )
        burned_data_gam = features.rasterize(
            shapes=shapes_gam,
            fill=0,
            out_shape=out_raster.shape,
            transform=out_raster.transform,
        )

        burned_data_sam = features.rasterize(
            shapes=shapes_sam,
            fill=0,
            out_shape=out_raster.shape,
            transform=out_raster.transform,
        )

        out_raster.write_band(1, burned_data_gam.astype(np.int32))
        out_raster.write_band(2, burned_data_sam.astype(np.int32))

        # Tag raster bands
        band_tags = {"band_1": "gam_population", "band_2": "sam_population"}
        out_raster.update_tags(**band_tags)
