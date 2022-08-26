import glob
import datetime

# import gdal
import geopandas as gpd
import numpy as np
import pandas as pd
from rasterstats import zonal_stats
import calendar


def clean_price_data(price_fn, sheet_names):
    price_dict = pd.read_excel(price_fn, sheet_name=sheet_names, header=None)
    for k, v in price_dict.items():
        markets = v.loc[v[0] == "Market", 2:].values.tolist()[0]
        data_start_ind = v.index[v[0] == "Year"][0]
        prices = v.iloc[data_start_ind:, :]
        prices.columns = prices.iloc[0, :]
        prices.drop(labels=prices.index[0], axis=0, inplace=True)
        prices["Month"] = prices["Month"].apply(
            lambda x: datetime.datetime.strptime(x, "%b").tm_mon
        )
        prices.set_index(["Year", "Month"], inplace=True)
        prices.sort_index(inplace=True)
        prices.columns = markets
        prices = prices.T.drop_duplicates().T
        price_dict[k] = prices
    return price_dict


def clean_price_df(price_fn, sheet_names):

    raw_df = pd.read_excel(price_fn, sheet_name=sheet_names, header=None)
    time_idx = raw_df.loc[raw_df[0] == "Year"].index.values[0]
    county_names = raw_df.iloc[1].values.tolist()[2:]
    county_names = [word.lower() for word in county_names]
    timept = raw_df[(time_idx + 1) :]  # noqa: E203 - ignore whitespace before ':'
    price_val = timept.iloc[:, 2:].values
    df_time = pd.DataFrame(
        {
            "Year": timept.iloc[:, 0].values.tolist(),
            "Month": timept.iloc[:, 1].values.tolist(),
        }
    )
    df_price = pd.DataFrame(data=price_val, columns=county_names)
    final_price_df = pd.concat([df_time, df_price], axis=1).T.drop_duplicates().T

    return final_price_df


def crop_data(dir_path):

    """ for example 'crop/*. """

    dir_list = glob.glob(dir_path + "*.csv")
    crop_df_total = []

    for i in dir_list:
        df = pd.read_csv(i)
        # filter out rows with NaN or State name
        # find rows where all columns are NaN
        inds = np.asarray(df.index[df.isnull().all(1)])
        state_idx = list(inds[:-1] + 1)
        df.iloc[state_idx] = np.nan
        df.dropna(inplace=True)
        # lowercase for count names
        cnames = []
        for _, row in df.iterrows():
            cnames.append(row["State/County"].strip().lower())
        df["State/County"] = cnames
        # print(cnames)
        df_trans = pd.DataFrame(
            df.values[:, 1:].T,
            columns=df["State/County"].tolist(),
            index=df.columns[1:].tolist(),
        )
        df_trans = df_trans.loc[:, ~df_trans.columns.duplicated()]
        crop_df_total.append(df_trans)
        df_total = pd.concat(crop_df_total, axis=0, sort=True)
        df_total.rename(
            columns={"canal/pigi": "canal pigi", "luakpiny/nasir": "nasir"},
            inplace=True,
        )
    return df_total


def mean_ndvi_stack(file_path, lag=1):

    file_dir = glob.glob(file_path + "*.csv")
    ndvi_df = []

    for i in file_dir:

        df = pd.read_csv(i)
        month_ls = list(df["Month"].dropna().unique())
        average_monthly = df.groupby(np.arange(len(df)) // 3).mean()[
            ["2014", "2015", "2016", "2017", "2018"]
        ]
        name = i.split("+")
        county_name = name[-1].split(".")[0].lower()
        df_month = pd.DataFrame({"Month": month_ls, "County": county_name})

        df_avg = pd.concat([df_month, average_monthly], axis=1)
        df_melted = pd.melt(
            df_avg,
            ["Month", "County"],
            value_vars=["2014", "2015", "2016", "2017", "2018"],
            value_name="NDVI",
            var_name="Year",
        )
        lag_name = "NDVI_lag" + str(lag)
        df_melted[lag_name] = df_melted["NDVI"].shift(lag)
        ndvi_df.append(df_melted)
    ndvi_df = pd.concat(ndvi_df, axis=0).reset_index(drop=True)
    return ndvi_df


def chirps_stack(file_path, lag=3):
    """
    Put a 3 month lags on the chirp measurement, where lag=3 (default)
    """

    file_dir = glob.glob(file_path + "*.csv")
    chirp_df = []
    for x in file_dir:
        df = pd.read_csv(x)
        df.drop("Unnamed: 12", axis=1, inplace=True)
        nan_indx = np.where(pd.isnull(df))

        for i in range(len(nan_indx[0])):
            stm_val = df["stm"][nan_indx[0][i]]
            for j in range(len(nan_indx[1])):
                df.iloc[nan_indx[0][i], nan_indx[1][j]] = stm_val

        monthly = df[["2013", "2014", "2015", "2016", "2017", "2018"]]
        month_ls = list(df["Month"].dropna().unique())
        name = x.split("+")
        county_name = name[-1].split(".")[0].lower()
        df_month = pd.DataFrame({"Month": month_ls, "County": county_name})
        df_avg = pd.concat([df_month, monthly], axis=1)
        df_melted = pd.melt(
            df_avg,
            ["Month", "County"],
            value_vars=["2013", "2014", "2015", "2016", "2017", "2018"],
            value_name="CHIRPS(mm)",
            var_name="Year",
        )
        lag_name = "CHIRPS(mm)_lag" + str(lag)
        df_melted[lag_name] = df_melted["CHIRPS(mm)"].shift(lag)
        chirp_df.append(df_melted)
    chirp_df = pd.concat(chirp_df, axis=0).reset_index(drop=True)
    return chirp_df


def estimate_pop(
    input_df, adjustment_rate=0, year=2020, birth_rate_fct=0, death_rate_fct=0
):
    """
    input_df:input file (i.e,/home/kimetrica/Code/darpa_population_model/data/south_sudan_rates.csv)
    adjustment_rate: option to adjust initial census data for 2008 only. (default = 0)
    year = range of integer values up to 2020 (default is 2020)
    birth_rate_fct/death_rate_fct: default to 0, it's the parameter to tweak for possible biases
    """
    pop_data = pd.read_csv(input_df)
    cname_low = []
    for _i, row in pop_data.iterrows():
        cname_low.append(row["ADMIN2NAME"].lower())

    pop_data["county_lc"] = cname_low

    pop_data["SS2008"] = pop_data["SS2008"] * (1 + adjustment_rate)
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
    return pop_data


def merge_rf_scenario(rainfall_scn_df, chirps_df):
    """
    This function merges the rainfal estimate into the chirps dataframe.
    rainfall_scn_df: dataframe with normalized/standardized column admin2
    chirps_df: dataframe of chirps df(already normalized columns)
    """
    rainfall_scn_df["month_lag3"] = rainfall_scn_df["month"] + 3
    rainfall_scn_df["month_lag3"] = rainfall_scn_df["month_lag3"].apply(
        lambda x: x - 12 if x > 12 else x
    )
    rainfall_scn_df["month_lag3"] = rainfall_scn_df["month_lag3"].apply(
        lambda x: calendar.month_abbr[x]
    )

    rainfall_scn_df.drop_duplicates(inplace=True)
    rain_pivot = rainfall_scn_df.pivot_table(
        index=["admin1", "admin2", "month_lag3"], columns="scenario", values="rainfall",
    ).reset_index()
    rain_pivot.rename(columns={"month_lag3": "Month"}, inplace=True)

    rain_chirp_merged = chirps_df.merge(rain_pivot, on=["admin1", "admin2", "Month"])
    """
    [
        [
            "admin1",
            "admin2",
            "County",
            "Population",
            "month_int",
            "crop_per_capita",
            "NDVI_lag1",
            "Year",
            "CPI",
            "gam_rate",
            "sam_rate",
            "med_exp",
            "CHIRPS(mm)_lag3",
            "high_rainfall",
            "low_rainfall",
            "mean",
        ]
    ]
    """

    return rain_chirp_merged


def df_aggregate(
    input_df,
    crop_input,
    pop_input,
    ndvi_input,
    chirps_input,
    expend_input,
    cpi_input,
    rev_cols,
):

    ndvi_cnames = list(ndvi_input["admin2"].unique())
    chirps_cnames = list(chirps_input["admin2"].unique())
    expend_cnames = list(expend_input["County"].unique())
    popdf_cnames = list(pop_input["county_lc"].unique())
    cropdf_cnames = list(crop_input["Admin2"].unique())
    # print(expend_cnames)
    pop_est = []
    crop_val = []
    ndvi_val = []
    cpi_idx = []
    chirp_val = []
    med_exp = []
    month = []
    month_num = []
    year = []
    admin1 = []
    admin2 = []
    input_df["County"] = rev_cols
    # in case of different spelling, check for this by replacing
    input_df["County"].replace(
        {
            "Canal Pigi": "canal/pigi",
            "Nasir": "luakpiny/nasir",
            "Kajo keji": "kajo-kejo",
            "canal pigi": "canal/pigi",
            "nasir": "luakpiny/nasir",
        },
        inplace=True,
    )

    for _i, row in input_df.iterrows():
        month.append(row["Month"])
        month_num.append(row["month_int"])
        year.append(row["Year"])
        c_name = row["County"].lower()

        # get crop per capita based on maize production from year prior:
        if c_name in cropdf_cnames:
            crop_yield = crop_input[
                (crop_input["Admin2"] == c_name) & (crop_input["Year"] == row["Year"])
            ]["crop_per_capita"].values[0]
            crop_val.append(crop_yield)
        else:
            print("this is not in crop_input: " + c_name)
            crop_val.append(np.nan)

        if c_name in popdf_cnames:
            # this needs to correspond to crop production (if crop is for 2015, year is 2016)
            yr = "SS" + str(row["Year"])
            pop_estval = pop_input[pop_input["county_lc"] == c_name][yr].values[0]
            pop_admin1 = pop_input[pop_input["county_lc"] == c_name][
                "ADMIN1NAME"
            ].values[0]
            pop_admin2 = pop_input[pop_input["county_lc"] == c_name][
                "county_lc"
            ].values[0]
            pop_est.append(pop_estval)
            admin1.append(pop_admin1)
            admin2.append(pop_admin2)

        else:
            print("this is not in pop_input: " + c_name)
            pop_est.append(np.nan)
            admin1.append(np.nan)
            admin2.append(np.nan)

        if c_name in ndvi_cnames:
            val = ndvi_input[
                (ndvi_input["Month"] == row["Month"])
                & (ndvi_input["admin2"] == c_name)
                & (ndvi_input["Year"] == row["Year"])
            ]["NDVI_lag1"].values[0]
            ndvi_val.append(val)
        else:
            print("this not in ndvi: " + c_name)
            ndvi_val.append(np.nan)

        if c_name in chirps_cnames:
            chp_val = chirps_input[
                (chirps_input["admin2"] == c_name)
                & (chirps_input["Year"] == row["Year"])
                & (chirps_input["Month"] == row["Month"])
            ]
            chirp_val.append(chp_val["CHIRPS(mm)_lag3"].values[0])

        else:
            print("this is not in chirps df: " + c_name)
            chirp_val.append(np.nan)

        if c_name in expend_cnames:
            expend_val = expend_input[expend_input["County"] == c_name][
                "med_expend"
            ].values[0]
            med_exp.append(expend_val)
        else:
            print("this is not in median expenditure df: " + c_name)
            med_exp.append(np.nan)

        # check to see if input_df has column named "rainfall" !
        # match_ls = input_df.columns.tolist()
        # if "rainfall" in match_ls:
        #  rain.append(row["rainfall"])
        # else:
        # rain.append(np.nan)

        # gdp_val.append(gdp[gdp['Country Name']=='South Sudan'][str(row['Year'])].values[0])
        cpi_yr = cpi_input.index[cpi_input["Year"] == str(row["Year"])].tolist()[0]
        cpi_val = cpi_input[row["Month"]][cpi_yr]
        cpi_idx.append(int(cpi_val.replace(",", "")))

    out_df = pd.DataFrame(
        {
            "admin1": admin1,
            "admin2": admin2,
            "Population": pop_est,
            "Month": month,
            "month_int": month_num,
            "crop_per_capita": crop_val,
            "NDVI_lag1": ndvi_val,
            "Year": year,
            "CPI": cpi_idx,
            "gam_rate": input_df["GAM_rate"] / 100,
            "sam_rate": input_df["SAM_rate"] / 100,
            "med_exp": med_exp,
            "CHIRPS(mm)_lag3": chirp_val,
            # "high_rainfall": high_rain,
            # "low_rainfall": low_rain,
            # "mean_rainfall": mean_rain,
        }
    )

    return out_df


def column_standardization(df, col_name, typo_dictionary):
    """
    fixes small typo and normalize spelling for Ethiopia Admin2 names
    """
    df[col_name] = df[col_name].str.strip()
    df.replace({col_name: typo_dictionary}, inplace=True)
    df[col_name] = df[col_name].str.lower()

    return df


def expenditure_merge(expend_df, main_df):
    """
    This function integrates Ethiopia PPP consumption expenditure (daily) into the main data frame
    expend_df: imported from the consumption expenditure csv (column 'expenditure_daily')
    main_df: the main dataframe used for training and prediction
    Note: due to the limited granularity of the expenditure data, for years before 2008, expenditure consumption
    from 2005/2006 will be used.
    For year after 2009,  expenditure consumption from 2010/2011 will be used.
    """
    exp_val = []
    for _i, row in main_df.iterrows():
        if row["Year"] < 2008:
            expend_daily = expend_df.loc[
                (expend_df["region"] == row["admin1"])
                & (expend_df["year"] == "2005/06")
            ]["expenditure_daily"]
            if not expend_daily.empty:
                exp_val.append(expend_daily.values[0])
            else:
                exp_val.append(np.nan)

        elif row["Year"] >= 2008:
            expend_daily = expend_df.loc[
                (expend_df["region"] == row["admin1"])
                & (expend_df["year"] == "2010/11")
            ]["expenditure_daily"]
            if not expend_daily.empty:
                exp_val.append(expend_daily.values[0])
            else:
                exp_val.append(np.nan)
        else:
            exp_val.append(np.nan)

    main_df["expenditure"] = exp_val
    return main_df


def eth_df_aggregate(
    inf_df_std, pop_df_std, ndvi_df_std, chirp_df_std, crop_df_std, cpi_df, expend_df
):
    # merge the inference variable template with population, ndvi, chirp_df, crop_df, cpi, do expenditure last
    # this works for year 2007-2016 due to limitation of data for Ethiopia
    merge1 = inf_df_std.merge(
        pop_df_std[["admin2", "Population", "Year"]], on=["admin2", "Year"]
    )
    merge2 = merge1.merge(
        ndvi_df_std[["admin2", "Year", "Month", "NDVI_lag1"]],
        on=["admin2", "Year", "Month"],
    )
    merge3 = merge2.merge(
        chirp_df_std[
            [
                "admin2",
                "Year",
                "Month",
                "CHIRPS(mm)_lag3",
                # "high_rainfall",
                # "low_rainfall",
                # "mean",
            ]
        ],
        on=["admin2", "Year", "Month"],
    )
    merge4 = merge3.merge(cpi_df, on=["Year", "Month"])
    # if inf_df_std["Year"].any() > 2018:
    # merge5 = merge4.copy()
    # merge5["crop_per_capita"] = np.nan
    # else:
    crop_df_std.drop_duplicates(
        subset=["Year", "Admin2", "Admin1", "crop_per_capita"], inplace=True
    )
    crop_df_std.rename(columns={"Admin2": "admin2", "Admin1": "admin1"}, inplace=True)
    merge5 = merge4.merge(crop_df_std, on=["Year", "admin1", "admin2"])

    aggreg_df = expenditure_merge(expend_df, merge5)
    aggreg_df.rename(columns={"expenditure": "med_exp"}, inplace=True)
    select_cols = [
        "admin1",
        "admin2",
        "Year",
        "NDVI_lag1",
        "Population",
        "CPI",
        "crop_per_capita",
        "CHIRPS(mm)_lag3",
        # "high_rainfall",
        # "low_rainfall",
        # "mean",
        "med_exp",
        "Month",
        "month_int",
    ]
    return aggreg_df[select_cols]


def mean_ndvi_kenya(file_path):

    file_dir = glob.glob(file_path + "*.csv")
    ndvi_df = []

    for i in file_dir:
        # print(i)
        df = pd.read_csv(i)
        month_ls = list(df["Month"].dropna().unique())
        average_monthly = df.groupby(np.arange(len(df)) // 3).mean()[
            ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"]
        ]
        name = i.split("+")
        county_name = name[-1].split(".")[0].lower()
        df_month = pd.DataFrame({"Month": month_ls, "County": county_name})

        df_avg = pd.concat([df_month, average_monthly], axis=1)
        df_melted = pd.melt(
            df_avg,
            ["Month", "County"],
            value_vars=[
                "2010",
                "2011",
                "2012",
                "2013",
                "2014",
                "2015",
                "2016",
                "2017",
                "2018",
            ],
            value_name="NDVI",
            var_name="Year",
        )
        ndvi_df.append(df_melted)
    ndvi_df = pd.concat(ndvi_df, axis=0).reset_index(drop=True)
    return ndvi_df


def crop_df_extraction(input_df):

    """
    this function extracts relevant crop data from the .csv crop production data from Fewsnet warehouse.
    Used mainly for Kenya crop(maize data)
    """

    h = input_df[
        [
            "admin_1",
            "admin_2",
            "period_date",
            "value",
            "unit_name",
            "population",
            "product_name",
        ]
    ]
    select_df = h[
        (h["product_name"] == "Maize Grain (White)") & (h["unit_name"] == "Tonne")
    ].reset_index()
    select_df["Year"] = pd.to_datetime(select_df["period_date"]).dt.year
    # select_df['crop_per_capita']=select_df['value']/select_df['population']
    county = []
    crop = []
    year = []
    population = []
    for _, row in select_df.iterrows():
        if row["Year"] < 2015:
            county.append(row["admin_2"].lower())
            crop_tonne = select_df[
                (select_df["Year"] == row["Year"])
                & (select_df["admin_2"] == row["admin_2"])
            ]["value"].sum()
            crop.append(crop_tonne)
            year.append(row["Year"])
            pop = select_df[
                (select_df["Year"] == row["Year"])
                & (select_df["admin_2"] == row["admin_2"])
            ]["population"].values[0]
            population.append(pop)

        if row["Year"] >= 2015:
            county.append(row["admin_1"].lower())
            crop_tonne = select_df[
                (select_df["Year"] == row["Year"])
                & (select_df["admin_1"] == row["admin_1"])
            ]["value"].sum()
            crop.append(crop_tonne)
            year.append(row["Year"])
            pop = select_df[
                (select_df["Year"] == row["Year"])
                & (select_df["admin_1"] == row["admin_1"])
            ]["population"].values[0]
            population.append(pop)

    output_df = pd.DataFrame(
        {"County": county, "Crop_tonne": crop, "Year": year, "population": population}
    )
    output_df["crop_per_capita"] = output_df["Crop_tonne"] / output_df["population"]
    output_df.drop_duplicates(
        subset=["crop_per_capita", "Year"], keep="first", inplace=True
    )
    return output_df


def KE_cpi(input_df):

    month = []
    year = []
    for _, row in input_df.iterrows():
        month.append(row["Time"].split(" ")[0])
        year.append(int(row["Time"].split(" ")[1]))
    input_df["Year"] = year
    input_df["Month"] = month
    pivot_df = input_df.pivot(
        index="Year", columns="Month", values="Overall CPI"
    ).reset_index()
    pivot_df.columns.name = None
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    pivot_int = (
        pivot_df[months].astype(float).fillna(-999).astype(int).replace({-999: np.nan})
    )
    pivot_df[months] = pivot_int
    return pivot_df


def ssd_df_revise(input_df):
    """
    this function is used to clean up the s.sudan malnutrition survey data
    """
    input_df["Year"] = input_df["Year"].astype(int)
    input_df["Month"] = pd.to_datetime(input_df["End date"]).dt.strftime("%b")
    input_df.rename(
        columns={
            "GAM (WHZ <-2 and/or oedema) 6-59 mo": "GAM_rate",
            "SAM (WHZ <-3 and/or oedema) 6-59mo": "SAM_rate",
        },
        inplace=True,
    )
    return input_df


def kenya_df_revise(kenya_df):
    """
    this function is used to clean up the kenya malnutrition survey data
    """
    month = []
    for _, row in kenya_df.iterrows():
        month.append(row["Month"][:3])

    kenya_df["Month"] = month
    kenya_df.rename(
        columns={
            "GAM (WHZ <-2 and/or oedema) 6-59 months": "GAM_rate",
            "SAM (WHZ <-3 and/or oedema) 6-59months": "SAM_rate",
        },
        inplace=True,
    )
    return kenya_df


def county_expend(shape_fname, tiff_fname):
    """
    This function takes the economic model output for consumption expenditure (S.Sudan) and /
    calculates median expenditure for the counties. It serves as one of the input vars for /
    malnutrition model. It assumes that the expenditure values predominantely varies spatially, /
    not taking temporal variation into account at this stage.
    """

    shape_df = gpd.GeoDataFrame.from_file(shape_fname)
    if "County" in shape_df.columns.tolist():
        shape_df.rename(columns={"County": "ADMIN2NAME"}, inplace=True)
    zones = shape_df[["ADMIN2NAME", "geometry"]]

    stats = zonal_stats(
        shape_fname, tiff_fname, stats="median", nodata=-9999, geojson_out=True
    )
    expenditure = []
    county = []
    for i, row in zones.iterrows():
        expenditure.append(stats[i]["properties"]["median"])
        county.append(row["ADMIN2NAME"].lower())
    zones.loc[:, "med_expend"] = expenditure
    zones.loc[:, "County"] = county
    zones["County"].replace(
        {
            # "canal/pigi": "canal pigi",
            # "luakpiny/nasir": "nasir",
            # "kajo-keji": "kajo keji",
            "panyijar": "panyijiar"
        },
        inplace=True,
    )
    return zones


def add_missing_dummy_columns(dummy_df):
    orig_columns = [
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
    missing_cols = set(orig_columns) - set(dummy_df.columns)
    for c in missing_cols:
        dummy_df[c] = 0
    return dummy_df


def malnutrition_mapping(census_df, malnutrition_df):
    """
    This function will merge the malnutrition prediction df with the census df. Malnutrition df
    needs to have 'County' as a column name.
    """
    # correct for typo
    typo = {
        "Abienmhom": "Abiemnhom",
        "Paniyijiar": "Panyijiar",
        "Southern Leer": "Leer",
        "Kapoeta North ": "Kapoeta North",
        "Twic East ": "Twic East",
        "Pariang ": "Pariang",
        "Kajo Keji": "Kajo-keji",
        "Canal Pigi": "Canal/Pigi",
    }
    malnutrition_df.replace({"County": typo}, inplace=True)

    malnut_cols = [
        "gam_rate",
        "sam_rate",
        "gam_cases",
        "sam_cases",
        "CHIRPS(mm)_lag3",
        "crop_per_capita",
        "Population",
        "Time",
        "Month",
        "Year",
        "County",
    ]

    census_cols = ["ADMIN2NAME", "ADMIN1NAME", "ADMIN1PCOD", "ADMIN2PCOD"]

    merged_df = pd.merge(
        malnutrition_df[malnut_cols],
        census_df[census_cols],
        how="left",
        left_on="County",
        right_on="ADMIN2NAME",
    )
    merged_df.rename(columns={"CHIRPS(mm)_lag3": "precipitation(mm)"}, inplace=True)
    return merged_df
