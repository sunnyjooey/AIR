import pandas as pd  # noqa: F401
import pickle


def remap_country_col(df, old_col_name, new_col_name):
    """
    maps old_col_Name to proper spellings of the country, then renames that column to new_col_name.
    """
    country_name_dict = pickle.load(  # noqa: S301
        open("models/conflict_model/functions/country_norm_name.pkl", "rb")
    )  # noqa: S301
    df.replace({old_col_name: country_name_dict}, inplace=True)
    df.rename(columns={old_col_name: new_col_name}, inplace=True)

    return df
