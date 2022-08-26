## Migration flow model

The migration flow model assesses migration sources, destinations, and volumes due to insecurity and conflict. It predicts the number of refugee from country of origin to country of destination in a given year on the national level. The input variables 
are collated from differen sources of data, and fitted to a Mixed Effect Random Forest Model (MERF). MERF is a modification of the Random Forest 
model that takes into account the random effect from cluster groups. In this case, the cluster group is defined as the pairing of origin-destination countries in 
variable `od`.

## The Data

The variables used to develop the model to predict number of refugees are listed below. The terms o and d refers to the availablity of the variable for origin and destination.
* **[o] refugee_count**: accessed from the UNHCR Population Statistics Database (popstats.unhcr.org). This is the target variable which consists of refugee counts recognized under the UNHCR definition as well as a refugee-like situation. It includes the number of refugees with a pair of countries of origin and destination for the years between 1951 and 2018.
* **[o] idps_count_lag**: the number of internally displaced people at origin country at time t-1. The reason for displacement is due to armed conflict. IDPs are differentiated from refugee counts as they have been forced to leave their homes but not crossed an international border. We used the previous year of IDPs as there could be time lag during transition from IDPs to refugees. The IDP data were also accessed from the UNHCR Population Statistics Database.
* **[o,d] population_count**: the estimated number of populations from World Bank Indicator Database (WDI) at origin and destination (https://data.worldbank.org/). 
* **[o,d] urban_population_percent**: a measure of the share of urban population at origin and destination (https://data.worldbank.org/).
* **[o] youth_percent**: the percent of youth population ages 15-24 to total population at origin country (https://data.worldbank.org/).
* **[o,d] corruption_perception_index**: this is an index that measures the level of corruption using perception of public sector corruption as determined by expert assessment and opinion surveys (https://www.transparency.org/). Transparency International defines corruption as ‘the misuse of public power to private benefit’, and it ranks 180 countries on a scale from 100 (very clean) to 0 (highly corrupt) (Transparency International, 2018).
* **[o] gdp_per_capita**: GDP per capita is gross domestic product divided by midyear population at origin (https://data.worldbank.org/).
* **[o, d] democracy_score**: a score measuring the degree between autocracy and  democracy at origin and destination country in a scale ranging from -10 to 10 (strong autocracy to strong democracy) (https://www.systemicpeace.org/).
* **[o] consumer_price_index**: Consumer price index reflects changes in the cost to the average consumer of acquiring a basket of goods and services that may be fixed or changed at specified intervals, such as yearly. Obtained from the World Bank indicator database and measures  the increase in the price level or inflation at origin (https://data.worldbank.org/).
* **[o] drought_risk_score**: a metric of drought risk that measures where droughts are likely to occur, the population and assets exposed, and the vulnerability of the population and assets to adverse effects (https://www.wri.org/).
* **[d] migrant_acceptance_index**: a three-item index using Gallup World Poll survey data from 140 countries. Migrant Acceptance Index ranges from 0 to 10 with item responses of  “a good thing”, “it depends” or “a bad thing” regarding questions about foreign migrants. 
* **[d] refugee_recognition_rate_lag**: the refugee recognition rate is the percent of refugees recognized out of the total number of asylum seekers at time t-1 using data form UNHCR Population Database (popstats.unhcr.org). This indicator is used as a proxy for favorable refugee policy where flexibility in recognition rate is expected to result in an increased number of refugees at destination.
* **[o, d] low_income_country**: a dummy variable representing 1 if a country is low or lower middle income according to World Bank classification.
* **[o, d] europe_and_north_america**: a dummy variable representing 1 if a country is located in Europe or North America.

## Data preprocessing
The data processing and aggregation are done in script `data_cleaning.py`.

## Outputs of the model
The relevant task output is `migration_pred_year` in script `tasks.py`. This task filters the result dataset based on `year` and `country_level` specified by the user in GlobalParameters. 
For example, this Luigi command line `luigi --module models.migration_model.tasks models.migration_model.tasks.migration_pred_year --time 2018-04-01-2018-06-01 --country-level Ethiopia --local-scheduler` 
returns prediction of refugees for year 2018, and for the country of Ethiopia whether it acts as origin or destination for refugree migration.


## Visualization 

This is a [mock up](http://refugee-movement.surge.sh/refugee-movement-map) of migration dashboard based on the dataset collected for the model.