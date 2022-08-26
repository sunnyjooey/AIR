def column_standardization(df, col_name, typo_dictionary):
    """
    fixes small typo and normalize spelling for Ethiopia Admin2 names
    """
    df[col_name] = df[col_name].str.strip()
    df.replace({col_name: typo_dictionary}, inplace=True)
    df[col_name] = df[col_name].str.lower()

    return df


def proj_val(df, yr_thresh=2000):
    df["year"] = df["year"].astype(int)
    pop_proj = df[
        (df["year"] >= yr_thresh) & (df["btotl"].notnull()) & (df["mtotl"].notnull())
    ]
    select_cols = [
        "admin0",
        "admin1",
        "admin2",
        "admin3",
        "datasourceid",
        "year",
        "census_or_projected",
        "btotl",
    ]
    pop_proj = pop_proj[select_cols]
    return pop_proj


def timelagged_val(
    df, admin_col="Admin 1", feat_col=("Total Cases", "log_cases"), lag=1
):
    import pandas as pd
    import numpy as np

    """
    Get previous case data with specified lag on column feat_col
    """
    df_lag = df.copy()
    for col in feat_col:
        lag_name = col + "_" + "lag" + str(lag)
        lag_val = []

        for row in df.itertuples():

            prev_date = row.timestamp - pd.DateOffset(months=lag)

            if (
                df.loc[
                    (df[admin_col] == row[2])
                    & (df["timestamp"] == prev_date)
                    & (df["Disease"] == row[3])
                ][col]
            ).any():

                prev_cases = df.loc[
                    (df[admin_col] == row[2])
                    & (df["timestamp"] == prev_date)
                    & (df["Disease"] == row[3])
                ][col].values[0]

                lag_val.append(prev_cases)

            else:
                lag_val.append(np.nan)
        df_lag[lag_name] = lag_val

    return df_lag
