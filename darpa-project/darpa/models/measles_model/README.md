## Model for predicting cases of measles
The measles model is a mixed-effect linear regression model that predicts the number of measles cases for South Sudan and Ethiopia. The model predicts the number of measles cases for the next month, and due to the autoregressive components of the model (discussed below), it can potentially forecast as a time series. 

The model itself can be summarized in this general equation[1]:

<div align="center">
    y=Xβ+Zu+ε 
</div>
 <p></p>
In this equation, X is a N×p matrix of the p independent fixed effect variables (like a normal linear model). β is px1 vector of weights/coefficients. The term Z is a Nxq matrix for q random effects from the grouping variable, and u is qx1 vector attributed to the random effects (analogous to β). In this case, the random effect resulted in different intercept for each category. The term ε refers to the error.

For this setup, the fixed effect variables correspond to input variables of # cases in the previous month, population, and the number of clinics on the admin1 level. The random effect variable is the Month, so the number of unique months is q for the vector u.

Since the population and number of clinics do not change significantly on a monthly basis, the model effectively acts as an autoregressive method on the number of cases from previous time point.

## The Data

* The raw data are collated in house from local government agencies for [South Sudan](https://data.kimetrica.com/dataset/28ed1647-dd84-42c8-89e9-5af8f4771112/resource/58c9ecde-ee63-4284-9221-c7422ed99a46) and [Ethiopia](https://data.kimetrica.com/dataset/28ed1647-dd84-42c8-89e9-5af8f4771112/resource/63465e2d-0936-4dbd-92b3-060cebd65157) at the *monthly* and *admin1* level. 
* The population data comes from Kimetrica's population model projection values.
* The clnics [data](https://data.kimetrica.com/dataset/663d3707-5512-4640-ae1f-fdeda4e61077/resource/779f6740-4973-47a7-90b9-fb5b127aa1a2) came from Humanitarian Data Exchange(HDX). 

## Input variables

* total_cases_lag1: number of measles cases from one month prior at the admin1 level.
* log_pop: population value from the admin1 level on the log scale.
* num_clinics: number of health clinics on the admin1 level
* Month: the month of interest. It is the month name in string format.

## Building the model

The model `smf.mixedlm` from statsmodels.formula.api is a mixed effect linear model that was used to train the data. Due to the limited availability of sample points, the model was trained on 106 samples, and validated on 10 samples. The r<sup>2</sup> value is 0.99, and Root Mean Squared Error (RMSE) is about 85.

## Outputs of the model

The output of the model is a geojson file that can be used as shapefile or Geopandas dataframe. It contains the input variables as well as the predicted `measles_number` and polygon coordinates.

## Quickstart code 

Due to the sparsity of data for cases of measles from the previous month(`total_cases_lag1`) , the user can only run certain time points for this model. For example, this terminal task can be executed and it will generate a geojson output: 
```bash
luigi --module models.measles_model.tasks models.measles_model.tasks.MeaslesPrediction \
--country-level Ethiopia --num-clinics-percent-change 0.0 --local-scheduler
```
## Constraints

The dataset is available from April, 2019 to February, 2020 (work in progress). The model does not accept NaN values.

#### REFERENCE

[1]Introduction to Linear Mixed Models
, https://stats.idre.ucla.edu/other/mult-pkg/introduction-to-linear-mixed-models/
[2]Imai,C, Armstrong,B, Chalabi,Z, et al, Time series regression model for infectious disease and weather,
Environmental Research, Volume 142, 2015, Pages 319-327,