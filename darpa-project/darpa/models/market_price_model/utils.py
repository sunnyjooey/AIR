import os
from functools import reduce

import geopandas as gpd
from datetime import datetime as dt
import pandas as pd


def merge_with_shapefile(csv, gdf, gdf_on):
    gdf.rename(columns={gdf_on: "Geography"}, inplace=True)
    gdf["Geography"] = gdf["Geography"].apply(lambda x: str(x).title())
    merged = csv.merge(gdf, on="Geography")
    merged = gpd.GeoDataFrame(merged, geometry="geometry")
    return merged


def merge_varying_geo_with_shapefile(csv, gdf, vgl, gdf_on):
    gdf.rename(columns={gdf_on: "Geography"}, inplace=True)
    remapping_dict = {}
    for i in vgl:
        for j in gdf["Geography"]:
            if j[: (len(i))] == i:
                remapping_dict[j] = i
    print(remapping_dict)
    gdf["GEO_for_join"] = gdf["Geography"]
    gdf.replace({"GEO_for_join": remapping_dict}, inplace=True)
    merged = csv.merge(gdf, left_on="Geography", right_on="GEO_for_join")
    merged = gpd.GeoDataFrame(merged, geometry="geometry")
    merged = merged.drop(columns=["Geography_x", "GEO_for_join"])
    gdf = gdf.drop(columns=["GEO_for_join"])
    merged.rename(columns={"Geography_y": "Geography"}, inplace=True)
    return merged


def merge_fat_kenya_varying_geo_with_shapefile(csv, gdf, vgl):
    remapping_dict1 = {}
    for region in vgl:
        editted_region = (
            region.replace("North", "")
            .replace("South", "")
            .replace("West", "")
            .replace("Central", "")
            .replace("East", "")
            .replace("Town", "")
            .replace("Hills", "")
        )
        remapping_dict1[region] = editted_region.strip()
    # print(remapping_dict1)
    csv.replace({"Geography": remapping_dict1}, inplace=True)
    remapping_dict = {}
    for i in remapping_dict1.values():
        for j in gdf["Geography"]:
            if j[: (len(i))] == i:
                remapping_dict[j] = i
    # print(remapping_dict)
    csv["Geography"] = csv["Geography"].replace("Homa Bay", "Homabay")
    csv["Geography"] = csv["Geography"].replace("Mt. Elgon", "Mt Elgon")
    csv["Geography"] = csv["Geography"].replace("Kitutu Masaba", "Masaba")
    csv["Geography"] = csv["Geography"].replace("Bomachoge Borabu", "Borabu")
    gdf["GEO_for_join"] = gdf["Geography"]
    gdf.replace({"GEO_for_join": remapping_dict}, inplace=True)
    merged = csv.merge(gdf, left_on="Geography", right_on="GEO_for_join")
    merged = gpd.GeoDataFrame(merged, geometry="geometry")
    merged = merged.drop(columns=["Geography_x", "GEO_for_join"])
    gdf = gdf.drop(columns=["GEO_for_join"])
    merged.rename(columns={"Geography_y": "Geography"}, inplace=True)
    # list(set(remapping_dict1.values()) - set(merged["Geography"].unique()))
    return merged


def str_to_num(s):
    if isinstance(s, int) or isinstance(s, float):
        return s
    else:
        return float(s.replace(",", ""))


def merge_independent_vars(parent_dir, independent_var_files):
    gdfs = []
    for f in independent_var_files:
        if f.endswith("geojson"):
            gdfs.append(gpd.read_file(os.path.join(parent_dir, f)))
    merged = reduce(
        lambda left, right: pd.merge(
            left,
            right[right.columns[right.columns != "geometry"]],
            on=["Geography", "Time"],
        ),
        gdfs,
    )
    return merged


def prepare_model_mats(commodity_dfs, vars):  # noqa: A002
    data_dict = {}
    for k, v in commodity_dfs.items():
        commodity = k.split(".")[0]
        cols = list(vars) + [f"p_{commodity}"]
        print(v)
        v["Time"] = pd.to_datetime(v["Time"])
        v.set_index(["Geography", "Time"], inplace=True)
        data = v[cols].shift(1)
        data.rename(columns={f"p_{commodity}": f"p_{commodity}_t-1"}, inplace=True)
        data.insert(0, column=f"p_{commodity}", value=v[f"p_{commodity}"])
        data.insert(0, column="geometry", value=v["geometry_x"])
        data_dict[commodity] = data
    return data_dict


def datetime_schema(gdf):
    schema = gpd.io.file.infer_schema(gdf)
    schema["properties"]["Time"] = "datetime"
    return schema


def convert_prices_df_to_dict(df):
    prices_dict = {}
    commodity_list = df["Commodity"].unique()
    for com in commodity_list:
        temp_datacut = df[df["Commodity"] == com]
        temp_datacut = temp_datacut.drop(columns=["Commodity"])
        if "Fuel" in com:
            comm_cat = ""
        else:
            comm_cat = "food_"
        temp_datacut.rename(columns={"Price": f"p_{comm_cat}{com}"}, inplace=True)
        prices_dict[f"{comm_cat}{com}"] = temp_datacut
    prices = prices_dict
    return prices


def uganda_geo_cleaning(df):
    # These are guesses, using the Admin2 Map when helpful
    df["Geography"] = df["Geography"].str.replace(" Municipality", "")
    df["Geography"] = df["Geography"].str.replace("Central Kampala", "Kampala")
    df["Geography"] = df["Geography"].str.replace("Kigulu", "Wakiso")
    df["Geography"] = df["Geography"].str.replace("Buruli M", "Nakasongola")
    df["Geography"] = df["Geography"].str.replace("Jie", "Kaabong")
    df["Geography"] = df["Geography"].str.replace("Labwor", "Abim")
    df["Geography"] = df["Geography"].str.replace("Dodoth", "Karenga")
    df["Geography"] = df["Geography"].str.replace("Upe", "Mpigi")
    df["Geography"] = df["Geography"].str.replace("Samia-bugwe", "Busia")
    return df


def sudan_geo_cleaning(df):
    df["Geography"] = df["Geography"].replace("Damazin", "El Damazeen")
    df["Geography"] = df["Geography"].replace("Jebal Aulya", "Jebel Awlya")
    df["Geography"] = df["Geography"].replace("Al Gedaref Rural", "Gedaref")
    df["Geography"] = df["Geography"].replace("Ed Daein", "El Dwaeem")
    df["Geography"] = df["Geography"].replace("Kadugli", "Kadougli")
    return df


def somalia_geo_cleaning(df):
    df["Geography"] = df["Geography"].replace("Bulo Burto", "Buulo Burdo")
    df["Geography"] = df["Geography"].replace("Bu'aale", "Bu'Aale")
    df["Geography"] = df["Geography"].replace("Belet Weyne", "Beled Weyn")
    df["Geography"] = df["Geography"].replace("Dhuusamarreeb", "Dhuusamareeb")
    df["Geography"] = df["Geography"].replace("Banadir", "Bander-Beyla")
    df["Geography"] = df["Geography"].replace("Gaalkacyo", "Gaalkacayo")
    df["Geography"] = df["Geography"].replace("Doolow", "Dolow")
    df["Geography"] = df["Geography"].replace("Laas Caanood", "Lascaanod")
    df["Geography"] = df["Geography"].replace("Jowhar", "Jawhar")
    df["Geography"] = df["Geography"].replace("Borama", "Boorama")
    df["Geography"] = df["Geography"].replace("Bossaso", "Bosaaso")
    df["Geography"] = df["Geography"].replace("Baydhaba", "Baydhabo")
    return df


def merge_global_oil_prices(df, global_oil_prices, adminx_boundaries):
    global_oil_prices = global_oil_prices.drop(global_oil_prices.index[0:1])
    global_oil_prices.rename(columns={0: "Time", 1: "Price"}, inplace=True)
    global_oil_prices["Time"] = global_oil_prices["Time"].apply(
        lambda x: dt.strptime(x, "%b-%y")
    )
    global_oil_prices["Time"] = global_oil_prices["Time"].apply(
        lambda x: dt.combine(x, dt.min.time())
    )
    global_oil_prices["Price"] = global_oil_prices["Price"].astype(float)

    agg = pd.DataFrame(
        df[["Time", "Geography", "geometry"]].groupby(["Time", "Geography"]).count()
    )
    agg = agg.reset_index()
    agg = agg.drop("geometry", axis=1)
    agg["Time"] = pd.to_datetime(agg["Time"])

    merged = global_oil_prices.merge(agg, on="Time")
    merged = merge_with_shapefile(merged, adminx_boundaries, gdf_on="adminx")
    if "GEO_for_join" in merged.columns:
        merged = merged.drop(columns=["GEO_for_join"])
    merged["Commodity"] = "Fuel - Crude Oil"

    commodity_list = df["Commodity"].unique()
    if any("Fuel" in string for string in commodity_list) is True:
        # NEED TO CHECK FOR OTHER COUNTRIES IF THEY CONTAIN FUEL OR OTHER GAS RELATED WORDS
        df = df[~df.Commodity.str.contains("Fuel")]
        # breakpoint()
    else:
        pass

    prices = pd.concat([df, merged])
    return prices
