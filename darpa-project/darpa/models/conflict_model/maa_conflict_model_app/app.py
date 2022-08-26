#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Mon Jan 25 17:46:54 2021
@author: yaredhurisa """

import datetime
import streamlit as st
import pandas as pd
import altair as alt

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
)

import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
import base64
import dill
import warnings
from shapely import wkt
import geopandas as gpd
from sklearn.ensemble import ExtraTreesClassifier

# url = "https://data.kimetrica.com/dataset/8c728bc7-7390-44c1-a99c-83c08b216d03/resource/262d427c-883a-4c8b-80e3-8fca5b3f97c5/download/myn_final_data_binary.csv"
# df = pd.read_csv(url, index_col=0)

df = pd.read_csv("myn_final_data_binary.csv").drop_duplicates(
    subset=["admin1", "admin2", "geometry", "month_year"]
)


@st.cache
def load_data(df):

    return (
        df,
        df.shape[0],
        df.shape[1],
    )


rows = df.shape[0]
columns = df.shape[1]
data = df[
    [
        "admin1",
        "admin2",
        "geometry",
        "month_year",
        "drought_index",
        "mean_rainfall",
        "pulses_price",
        "rice_price",
        "longitude",
        "latitude",
        "mining_area_log",
        "pop_density",
        "urban_pop",
        "lc",
        "youth_bulge",
        "years_schooling",
        "poverty",
        "tv",
        "stunting",
        "gender_index",
        "wasting",
        "road_density",
        "ethnicty_count",
        "actor_gf",
        "cc_frequency",
        "actor_rf",
        "cc_onset_x",
        "cellphone",
        "battles",
        "electricity",
        "infant_mortality",
        "patrilocal_index",
        "m_rebels",
        "remote_violence",
        "actor_c",
        "fatalities",
        "fatalities_per_event",
        "s_protesters",
        "protests",
        "violence",
        "actor_p",
        "m_civilians",
        "actor_pm",
        "sd",
        "pm_civilians",
        "r_civilians",
        "s_military",
        "m_p_militias",
        "r_rebels",
        "s_p_militias",
        "actor_r",
        "riots",
        "m_protesters",
        "cc_onset_y",
    ]
]

end_date = "2019-01"
mask = data["month_year"] < end_date
train1 = data.loc[mask]


start_date = "2018-12"
end_date = "2020-01"
mask = (data["month_year"] > start_date) & (data["month_year"] < end_date)
test1 = data.loc[mask]


end_date = "2020-01"
mask = data["month_year"] < end_date
re_train1 = data.loc[mask]


start_date = "2020-12"
end_date = "2022-01"
mask = (data["month_year"] > start_date) & (data["month_year"] < end_date)
current = data.loc[mask].drop(["cc_onset_y"], axis=1)

train = train1.drop(["admin1", "admin2", "geometry", "month_year"], axis=1)
re_train = re_train1.drop(["admin1", "admin2", "geometry", "month_year"], axis=1)
test = test1.drop(["admin1", "admin2", "geometry", "month_year"], axis=1)
current1 = current.drop(["admin1", "admin2", "geometry", "month_year"], axis=1)

X_train = train[train.columns[:-1]]
X_test = test[test.columns[:-1]]
X_re_train = re_train[train.columns[:-1]]
y_train = train.cc_onset_y
y_test = test.cc_onset_y
y_re_train = re_train.cc_onset_y

X_current = current1

current.to_csv("new_data_forecasting.csv")


def home_page_builder(df, data, rows, columns):
    st.title("Kimetrica Conflict Forecasting Model: Myanmar")
    st.write("")
    st.write("")
    st.subheader("INTRODUCTION")
    st.write("")
    st.write(
        "An early-warning system that can meaningfully forecast conflict in its various forms is necessary to respond to crises ahead of time."
        " The ability to predict where and when conflict is more likely to occur will have a significant impact on reducing the devastating consequences of conflict."
        " The goal of this conflict model is to forecast armed conflict over time and spacein Myanmar at the second administrative level and on a monthly basis."
        " This document will outline the model construction methodology and the model output."
    )
    st.write("")
    st.write(
        "Most predictive models for conflict use country-level data in yearly time increments (Aas Rustad et al., 2011)."
        " One problem with this type of analysis is that it assumes that conflict is distributed uniformly throughout the country and uniformly throughout the year."
        " This situation is rarely the case as conflict usually takes place on the borders of countries."
        " For a model to be maximally useful, it must predict where in the country the conflict is likely to occur."
        " Likewise, for a model to be useful for decision-makers, it must be able to predict when the conflict will occur (Brandt et al., 2011)."
    )
    st.write("")
    st.write(
        "To satisfy the requirements of the MAA project, we have built a model to predict conflict at the county (admin2) level at monthly time intervals one year into the future."
        " This application presents the steps taken to build the model, visualize the data and result , run the model and model performance. "
    )
    st.write("")
    st.write("")
    st.subheader("INSTRUCTION")
    st.write("")
    st.write(
        "This website runs the conflict model and the associated pages that are useful for the users to understand the model outputs."
        " The navigation buttons are provided in the drop down list under the main menu."
        " The Home button represents the current page."
        " You can navigate between pages by clicking a list of buttons including the page to run the model."
    )
    st.write("")
    st.write("")


df2 = df.drop(
    ["Unnamed: 0", "Unnamed: 0.1", "admin1", "admin2", "geometry", "location", "year"],
    axis=1,
)

end_date = "2021-01"
mask = df2["month_year"] < end_date
df2 = df2.loc[mask]
df3 = df2.drop(["month_year"], axis=1)
X = df3[df3.columns[:-1]]
y = df3[df3.columns[-1]]
model = Pipeline(
    [("StandardScaller", StandardScaler()), ("RF", ExtraTreesClassifier())]
)
model.fit(X, y)
feat_importances = model.named_steps["RF"].feature_importances_
most_important = dict(
    sorted(
        dict(zip(X.columns, feat_importances)).items(), key=lambda x: x[1], reverse=True
    )
)
fp = pd.DataFrame(list(most_important.items()))
vip = dict(sorted(most_important.items(), key=lambda x: x[1], reverse=True))


def model_description_page_builder():
    st.title("Kimetrica Conflict Forecasting Model: Myanmar")
    st.write("")
    st.write("")
    st.subheader("MODEL DESCRIPTION")
    st.write("")
    st.write(
        "The conflict data has two distinct features that require special care compared to conventional machine learning problems."
        " These are class imbalance and recurrence."
    )
    st.write("")
    st.subheader("Class imbalance")
    st.write("")
    st.write(
        "In reality, conflict occurs in a rare situation resulting in a significant class imbalance in the output data between conflict and non-conflict events."
        " As can be seen from the following chart, overall, the percent of positive records for conflict ranges between 20 and 40 percent for most of the years."
        " This requires a mechanism that can take into account for the less number of positive(conflict) records in the dataset."
    )
    st.write("")
    if st.checkbox("Show class imbalance"):
        source = df.groupby(["year", "cc_onset_y"])["admin1"].count().reset_index()

        c_onset_chart = (
            alt.Chart(source, title="Number of conflict records by year")
            .mark_bar(size=20)
            .encode(
                alt.X("year:O", title="year"),
                alt.Y("admin1", title="percent of records"),
                alt.Color("cc_onset_y:O", legend=alt.Legend(title="conflict Status")),
            )
            .properties(width=500)
        )
        st.altair_chart(c_onset_chart)
    st.write("")
    st.subheader("Recurrance")
    st.write("")
    st.write(
        "The second aspect of the conflict event dataset is that, once conflict occurs, it has a tendency to last for an extended number of months and years."
        " As such, the model needs to have the capacity to trace recurrence."
        " CFM handles this issue by incorporating a threshold of probability of confidence in claiming the events."
        " In this case, the model takes the current situation if the confidence level drops less than the average mean difference."
    )
    st.write("")
    st.subheader("EasyEnsemble classifier")
    st.write("")
    st.write(
        "Undersampling is among the popular methods of handling class-imbalance."
        " This method entails taking a subset of the major class to train the classifier."
        " However, this method has a main deficiency as it ignores portions of the dataset in an attempt to balance the number of positive records."
    )
    st.write("")
    st.write(
        "Xu-Ying, Jianxin, and Zhi-Hua (2080), proposed EasyEnsemble classifier to overcome the above problem of under sampling."
        " EasyEnsemble forecast samples several subsets from the majority class and combines for a final decision."
        " These independent samples ultimately take into account the different aspects of the entire dataset."
    )
    st.write("")
    st.subheader("Output data")
    if st.checkbox("View output variables"):
        st.write(
            "* `cc_onset_y`: is our target variable representing conflict in a binary (0, no conflict; 1, conflict) and probability format."
        )
    st.subheader("Input data")
    if st.checkbox("View input variables"):
        st.write(
            "* `cc_onset_x`: current and previous conflict at admin2 level. Data comes from ACLED compiled on a monthly."
        )
        st.write("")
        st.write("* `cellphone`: household access to cell phones")
        st.write("")
        st.write("* `electricity`: household access to electricity")
        st.write("")
        st.write("* `ethnicty_count`: number of ethnic groups")
        st.write("")
        st.write("* `fatalities`: number of fatalities due to conflict")
        st.write("")
        st.write("* `gender_index`: gender index")
        st.write("")
        st.write("* `infant_mortality`: infant mortality rate ")
        st.write("")
        st.write("* `lc`: landuse change index")
        st.write("")
        st.write("* `mean_rf`: average monthly rainfall")
        st.write("")
        st.write("* `patrilocal_index`: patriolocal index")
        st.write("")
        st.write("* `pop_density`: number of people per KM2")
        st.write("")
        st.write("* `poverty`: percent of poor households")
        st.write("")
        st.write("* `rice_price`: monthly rice price")
        st.write("")
        st.write("* `stunting`: percentage of stunted children ")
        st.write("")
        st.write("* `tv`: household access to tv ")
        st.write("")
        st.write("* `urban_pop`: percent of population in urban areas")
        st.write("")
        st.write("* wasting`: percentage of wasted children")
        st.write("")
        st.write("* `pulses_price`: monthly pulses price")
        st.write("")
        st.write("* `years_schooling`: mean years of schooling ")
        st.write("")
        st.write(
            "* `youth_buldge`: proportion of working age group to the active population"
        )
        st.write("")
        st.write("* `drought_risk`: evaporative stress index (4 week)")
    st.subheader("Feature Importances")
    if st.checkbox("View feature importances"):
        source = pd.DataFrame(
            {"Feature": list(vip.keys())[:20], "Importance": list(vip.values())[:20]}
        )

        feature_importance_chart = (
            alt.Chart(source, title="Twenty most important predictors of conflict")
            .mark_bar()
            .encode(
                x="Importance:Q",
                y=alt.Y("Feature:N", sort="-x"),
                color="Feature",
                tooltip=["Feature", "Importance"],
            )
            .properties(width=500)
        )

        st.altair_chart(feature_importance_chart)


def logistic_train_metrics(df):
    """Return metrics and model for Logistic Regression."""

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        model_reg = dill.load(open("maa_conflict_model.dill", "rb"))  # NOQA: S301

    return model_reg


model_reg = logistic_train_metrics(df)
y_pred = model_reg.predict(X_test)
y_pred = pd.DataFrame(y_pred.astype(int))
y_pred.rename(columns={0: "cc_onset_prediction"}, inplace=True)
df_test = test1.reset_index()
df_evl = df_test.join(y_pred)
df_evl1 = df_evl[
    ["admin1", "admin2", "geometry", "month_year", "cc_onset_y", "cc_onset_prediction"]
]
df_evl1.cc_onset_y = df_evl1.cc_onset_y.astype(int)
cc_onset_actual = df_evl1.pivot(
    index=["admin1", "admin2", "geometry"], columns="month_year", values="cc_onset_y"
)
cc_onset_actual.columns = cc_onset_actual.columns.get_level_values("month_year")
cc_onset_actual.columns = [
    "".join(col).strip() for col in cc_onset_actual.columns.values
]
cc_actual = cc_onset_actual.reset_index()
cc_actual["2019"] = cc_actual.iloc[:, 3:].sum(axis=1)
cc_actual = cc_actual[
    [
        "admin1",
        "admin2",
        "geometry",
        "2019-01",
        "2019-02",
        "2019-03",
        "2019-04",
        "2019-05",
        "2019-06",
        "2019-07",
        "2019-08",
        "2019-09",
        "2019-10",
        "2019-11",
        "2019-12",
        "2019",
    ]
]
cc_actual["geometry"] = cc_actual["geometry"].apply(wkt.loads)

cc_actual = gpd.GeoDataFrame(cc_actual, geometry="geometry")


cc_onset_prediction = df_evl1.pivot(
    index=["admin1", "admin2", "geometry"],
    columns="month_year",
    values="cc_onset_prediction",
).reset_index()
cc_onset_prediction.columns = cc_onset_prediction.columns.get_level_values("month_year")
cc_onset_prediction.columns = [
    "".join(col).strip() for col in cc_onset_prediction.columns.values
]
cc_prediction = cc_onset_prediction.reset_index()
cc_prediction["2019"] = cc_onset_prediction.iloc[:, 3:].sum(axis=1)
cc_prediction = cc_prediction[
    [
        "admin1",
        "admin2",
        "geometry",
        "2019-01",
        "2019-02",
        "2019-03",
        "2019-04",
        "2019-05",
        "2019-06",
        "2019-07",
        "2019-08",
        "2019-09",
        "2019-10",
        "2019-11",
        "2019-12",
        "2019",
    ]
]
cc_prediction["geometry"] = cc_prediction["geometry"].apply(wkt.loads)
cc_prediction = gpd.GeoDataFrame(cc_prediction, geometry="geometry")


def logistic_page_builder(model_reg, X_test):
    st.title("Kimetrica Conflict Forecasting Model: Myanmar")
    st.subheader("TRAIN AND TEST")
    start_time = datetime.datetime.now()
    # model_reg = logistic_train_metrics(data)

    st.write(
        "In this page, you will be able to view model performance results(error matrix and classification report)."
        " You can also visualize actual vs predicted conflict on annual and monthly basis."
    )

    st.write(
        f"The model took a total running time of {(datetime.datetime.now() - start_time).seconds} s."
    )

    if st.checkbox("Show model error matrix"):

        conf_ee = confusion_matrix(y_test, y_pred)
        group_names = ["True Neg", "False Pos", "False Neg", "True Pos"]
        group_counts = ["{0:0.0f}".format(value) for value in conf_ee.flatten()]
        group_percentages = [
            "{0:.2%}".format(value) for value in conf_ee.flatten() / np.sum(conf_ee)
        ]
        labels = [
            f"{v1}\n{v2}\n{v3}"
            for v1, v2, v3 in zip(group_names, group_counts, group_percentages)
        ]
        labels = np.asarray(labels).reshape(2, 2)
        fig, ax = plt.subplots()
        ax = plt.axes()
        st.write(
            sns.heatmap(
                conf_ee,
                annot=labels,
                fmt="",
                cmap="Blues",
                xticklabels=["No Conflict", "Conflict"],
                yticklabels=["No Conflict", "Conflict"],
                ax=ax,
            )
        )
        ax.set_title("Final Model Error Matrix")
        sns.set(font_scale=0.5)
        st.pyplot(fig)
    if st.checkbox("Show classification report"):
        st.subheader("Classification Report")
        report = classification_report(y_test, y_pred)
        st.write(report)

    if st.checkbox("Visualize actual vs predicted conflict"):

        if st.checkbox("2019: 12 months"):
            fig, axes = plt.subplots(ncols=2)
            ax = plt.subplots()
            ax = cc_actual.plot(column="2019")
            axes[0].set_title("Actual")
            axes[1].set_title("Predicted")
            axes[1].legend(title="Months in conflict", loc="upper right")
            cc_actual.plot(column="2019", cmap="OrRd", ax=axes[0])
            cc_prediction.plot(column="2019", cmap="OrRd", legend=True, ax=axes[1])
            st.pyplot(fig)

        if st.checkbox("2019-01"):
            fig, axes = plt.subplots(ncols=2)
            ax = plt.subplots()
            ax = cc_actual.plot(column="2019-01")
            axes[0].set_title("Actual")
            axes[1].set_title("Predicted")
            axes[1].legend(title="Months in conflict", loc="upper right")
            cc_actual.plot(column="2019-01", cmap="OrRd", ax=axes[0])
            cc_prediction.plot(column="2019-01", cmap="OrRd", legend=True, ax=axes[1])
            st.pyplot(fig)
        if st.checkbox("2019-02"):
            fig, axes = plt.subplots(ncols=2)
            ax = plt.subplots()
            ax = cc_actual.plot(column="2019-02")
            axes[0].set_title("Actual")
            axes[1].set_title("Predicted")
            axes[1].legend(title="Months in conflict", loc="upper right")
            cc_actual.plot(column="2019-01", cmap="OrRd", ax=axes[0])
            cc_prediction.plot(column="2019-01", cmap="OrRd", legend=True, ax=axes[1])
            st.pyplot(fig)


columns = X_train.shape[1]


def new_data_downloader(df):

    st.write("")

    st.subheader("Want to upload new data to perform forecasting?")
    if st.checkbox("New data"):

        csv = current.to_csv(index=False)
        # some strings <-> bytes conversions necessary here
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
        st.markdown(href, unsafe_allow_html=True)

        st.write("")
        st.subheader("Want to download the new dataset to perform forecasting?")
        csv = current.to_csv(index=False)
        # some strings <-> bytes conversions necessary here
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
        st.markdown(href, unsafe_allow_html=True)


def file_uploader(uploaded_file):
    st.file_uploader("Choose a CSV file", type="csv")
    uploaded_file = pd.read_csv(uploaded_file, low_memory=False)
    st.text("This process probably takes few seconds...")
    return uploaded_file


def logistic_predictor():
    st.title("Kimetrica Conflict Forecasting Model: Myanmar")
    st.subheader("FORECAST")
    st.write(
        "This page enables you to make forecasting by uploading system generated or user defined dataset."
    )
    st.write(" Please check the following box to perform forecasting and view the data")

    if st.checkbox("Do you want to upload your own data?"):

        st.write(
            "Note: Currently, the file to be uploaded should have **exactly the same** format with **training dataset**"
            f" which is **{current.shape[1]}** columns in the following format.",
            current.head(2),
        )

        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

        if st.checkbox("Preview uploaded data"):
            uploaded_file = pd.read_csv(
                uploaded_file, low_memory=False, index_col=0
            ).drop_duplicates(subset=["admin1", "admin2", "geometry", "month_year"])
            st.write("Uploaded data:", uploaded_file.head())

            st.write("-" * 80)
            st.text(f"Uploaded data includes {uploaded_file.shape[1]} columns")
            st.write("-" * 80)
            # start_time = datetime.datetime.now() # 'start_time' is assigned to but never used

    if st.checkbox("Forecast and preview the results with the available data"):
        if st.checkbox("Preveiw the data with forecasted values"):

            y_forecast_binary = model_reg.predict(X_current)
            current["conflict_forecast_binary"] = [
                "No conflict" if i == 0 else "Conflict" for i in y_forecast_binary
            ]
            y_forecast_proba = model_reg.predict_proba(X_current)[:, 1]

            current["conflict_forecast_probability"] = y_forecast_proba.tolist()

            st.write(current.head(10))
            if st.checkbox("Visualize conflict forecast in a binary format"):
                df_evl1_b = current[
                    [
                        "admin1",
                        "admin2",
                        "geometry",
                        "month_year",
                        "conflict_forecast_binary",
                    ]
                ]

                cc_onset_actual = df_evl1_b.pivot(
                    index=["admin1", "admin2", "geometry"],
                    columns="month_year",
                    values="conflict_forecast_binary",
                ).reset_index()
                cc_onset_actual.columns = cc_onset_actual.columns.get_level_values(
                    "month_year"
                )
                cc_onset_actual.columns = [
                    "".join(col).strip() for col in cc_onset_actual.columns.values
                ]
                cc_actual = cc_onset_actual.reset_index()
                cc_actual["2021"] = cc_actual.iloc[:, 3:].sum(axis=1)

                cc_actual["geometry"] = cc_actual["geometry"].apply(wkt.loads)

                cc_forecast = gpd.GeoDataFrame(cc_actual, geometry="geometry")
                if st.checkbox("2021: First Quarter-binary"):
                    fig, axes = plt.subplots(ncols=4)
                    # ax = plt.subplots() # local variable 'ax' is assigned to but never used
                    axes[0].set_title("2021-01")
                    axes[1].set_title("2021-02")
                    axes[2].set_title("2021-03")
                    axes[3].set_title("2021-04")

                    axes[3].legend(title="Probability of conflict", loc="upper right")
                    cc_forecast.plot(
                        column="2021-01", cmap="OrRd", ax=axes[0], legend=True
                    )
                    cc_forecast.plot(
                        column="2021-02", cmap="OrRd", ax=axes[1], legend=True
                    )
                    cc_forecast.plot(
                        column="2021-03", cmap="OrRd", ax=axes[2], legend=True
                    )
                    cc_forecast.plot(
                        column="2021-04", cmap="OrRd", ax=axes[3], legend=True
                    )

                    st.pyplot(fig)

            if st.checkbox("Visualize conflict forecast in a probability format"):
                df_evl1_p = current[
                    [
                        "admin1",
                        "admin2",
                        "geometry",
                        "month_year",
                        "conflict_forecast_probability",
                    ]
                ]

                cc_onset_p = df_evl1_p.pivot(
                    index=["admin1", "admin2", "geometry"],
                    columns="month_year",
                    values="conflict_forecast_probability",
                ).reset_index()
                cc_onset_p.columns = cc_onset_p.columns.get_level_values("month_year")
                cc_onset_p.columns = [
                    "".join(col).strip() for col in cc_onset_p.columns.values
                ]
                cc_forecast_p = cc_onset_p.reset_index()

                cc_forecast_p["geometry"] = cc_forecast_p["geometry"].apply(wkt.loads)

                cc_forecast_p = gpd.GeoDataFrame(cc_forecast_p, geometry="geometry")
                if st.checkbox("2021: First Quarter-probability"):
                    fig, axes = plt.subplots(ncols=4)
                    # ax = plt.subplots() #  'ax' is assigned to but never used
                    # ax = cc_forecast_p.plot(column="2021-01")
                    axes[0].set_title("2021-01")
                    axes[1].set_title("2021-02")
                    axes[2].set_title("2021-03")
                    axes[3].set_title("2021-04")

                    axes[3].legend(title="Probability of conflict", loc="upper right")
                    cc_forecast_p.plot(
                        column="2021-01", cmap="OrRd", ax=axes[0], legend=True
                    )
                    cc_forecast_p.plot(
                        column="2021-02", cmap="OrRd", ax=axes[1], legend=True
                    )
                    cc_forecast_p.plot(
                        column="2021-03", cmap="OrRd", ax=axes[2], legend=True
                    )
                    cc_forecast_p.plot(
                        column="2021-04", cmap="OrRd", ax=axes[3], legend=True
                    )

                    st.pyplot(fig)


def main():
    """Application of Kimetrica Conflict Forecasting Model: Myanmar Analytical Activity (MAA)"""

    st.sidebar.title("Menu")
    choose_model = st.sidebar.selectbox(
        "Choose the page or model",
        [
            "Home",
            "Model description",
            "Train and Test",
            "Forecast and Visualize results",
            "Comment",
        ],
    )

    # Home page building
    if choose_model == "Home":
        home_page_builder(df, data, rows, columns)
        # Home page building
    if choose_model == "Model description":
        model_description_page_builder()

    # Page for Logistic Regression
    if choose_model == "Train and Test":
        model_reg = logistic_train_metrics(X_test)
        logistic_page_builder(model_reg, X_test)

        # Home page building
    if choose_model == "Forecast and Visualize results":

        logistic_predictor()

        # Home page building
    if choose_model == "Comment":
        st.title("Kimetrica Conflict Forecasting Model: Myanmar")
        st.subheader("PLEASE PROVIDE YOUR COMMENT")
        st.write("This page enables you to provide a short feedback about the app.")

        user_input = st.text_area("your comment goes here")
        user_input


if __name__ == "__main__":
    main()
