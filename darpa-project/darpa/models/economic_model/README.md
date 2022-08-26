## Household Economic Model

The Household Economic Model predicts individual consumption expenditure 
values in the units of dollars per person per day. Consumption expenditure 
is calculated from household level survey data. The survey data can be 
Living Standards Measurement Study (LSMS), a World Bank High Frequency 
Survey, or any other survey that contains a full consumption expenditure 
module that provides data on expenditure, consumption, and demographics of 
each household. This model a employs the random forest algorithm and uses 
globally-available raster data layers as the explanatory variables.

### The Data

- Global annual average luminosity from 2015 as measured by
  [NOAA]. The product is measured at 1km resolution. There does appear to be
  [monthly data] that is updated at the end of the following month. Right 
  now the model pulls only the 2015 data, from where it is hosted on 
  [CKAN][night_lights]. Exploring a more dynamic data pull would be good.
    - Variable name: `night_lights`

- Using topography and transport networks, including rods,
  rivers, and railroads, the accessiblity surface estimates the travel time 
  required to reach the nearest town of 100,000+ people. The model originally 
  used the accessibility map published by the [Malaria Map Project] for 2015.
  The full methodology can be found in this [Weiss et al. (2018)] paper in 
  Nature, also available in this open source [pdf version]. The surface is 
  calculated at 1km resolution. We have since developed our
  own [accessibility surface model] that includes a more accurate road 
  network for East Africa, can be updated to reflect any month or year of 
  interest, and which allows perturbations to affect the surface. For 
  example, if flooding inundates roads, we can reflect the impassability in 
  the model to re-calculate travel times.
    - Variable name: `accessibility`

- A USGS product that classifies global landover for 2015.  The 
  original data has since been taken off of the USGS website but can be found
  in our [CKAN database][landcover]. USGS points to [many additional 
  landcover products] that can be explored for future use.
    - Variable name: `landcover`

- Annual population density at the 1km resoultion as 
  produced by our [population model]. Documentation on the product can be 
  found in that repository.
    - Variable name: `population`

### Data Pre-processing

There are two files that execute crucial data pre-processing. The first step
deals with data extraction from the explanatory variables while the second
deals with calculating consumption expenditure.

### Geodata Extraction

In order to match the household consumption expenditure data with the
explanatory variables, we must extract data from the explanatory data layers
based on the geographic location of the households from the survey. Most 
survey data releases GPS data at the cluster level rather than the individual
household level. A cluster has a number (~10-30) of hosueholds in it. Often 
the centroid of the cluster is given jittered by a 5km or 10km buffer. This 
buffer is meant as a security and privacy measure to protect the individual 
households who participated in the survey. 

The `x_data_prep.py` file contains the pipeline to extract the data 
from each raster layer. Based on the buffer and the GPS centroid of the 
cluster, the `ExtractClusterData` task masks the explanatory variable raster
data layers to obtain input data for the model. This process must be 
repeated if there is any change in the underlying explanatory data layers. 
For example, if the time period or geography is changed or if increased 
rainfall affects the accessibility data layer, then this data pre-processing
must be re-run to re-extract the input data for the model.

## South Sudan Use Case

The first country this model was applied to was South Sudan.  Four waves of
data were collected by the world bank between 2015-2017 in a High Frequency
Survey.  The data can be found on CKAN: [Wave 1], [Wave 2], [Wave 3], [Wave 
4]. The geographic data was not public in any form for this survey. We
were able to obtain the centroid GPS points of the household clusters by
reaching out to the survey designers and signing an NDA to obtain the data.
However, this means that the data still cannot be made public and only those
with a local copy of the GPS data will be able to run the model for this
location. This may be a common issue with socioeconomic survey data.

## Consumption Expenditure

Consumption expenditure must be calculated from the survey data, translated 
into the proper units, and aggregated to the cluster level so that it can be
merged with the explanatory variables (as described above). The
`y_data_prep.py` file contains the pipeline for calculating and aggregating
consumption expenditure data.

All price data is imported and converted into USD from the local prices 
based on the exchange rate during the month of data collection in the
`NormalizeFoodPriceData` task. Then the household roster is used to
calculate the number of adult equivalent members in the household in the
`CalculateAdultEquivalence` task. Adult equivalence normalizes across
gender and age to assign the number of adult equivalent units living in the
household. The normalization is based on the variable caloric intake
required by people based on age and gender. Consumption expenditure can
then be calculated in the `CalculateConsumptionExpenditure` task. The exact
grouping of food items into food groups must be determined by the modeler.
It will likely be determined by international standards/guidelines and
what data is available. In the South Sudan use case, the food groupings
were determined by the [Food Consumption Score] food groupings. The output
of that task will give the consumption expenditure, per day, per adult
equivalent in the household.

The consumption expenditure data is then merged with the explanatory
variable data. The final output from the `OutputTrainingData` task data is a
csv file containing a dataframe that is ready to be fed into the model for
training. It is uploaded into a [resource in CKAN].

## Running the Model

Run the model with default parameters:

```bash
luigi --module models.economic_model.tasks \
  models.economic_model.tasks.Predict \
  --local-scheduler
```

Run the model with parameters setting the spatial bounds to cover Ethiopia:

```bash
luigi --module models.economic_model.tasks \
  models.economic_model.tasks.Predict \
  --country-level "Ethiopia" \
  --local-scheduler
```

The `tasks.py` file takes all of the pre-processed, pre-assembled data to
train a random forest model. The `Train` task sets all of the
hyperparameters and trains the model. Once the model has been trained, the
model is run over the entire surface defined by the explanatory variables to
produce a prediction surface at the 1km level. The final surface is output
as a raster.

## Distributing Results (Under Development)

The gridded data should be aggregated by administrative units and the combined
outputs (or links to outputs) conveyed in a JSON object. Run the same parameters
on the `Combine` task in the `models/economic_model/output` module to send
the JSON object to an S3 bucket.


[NOAA]: https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10/2015/
[monthly data]: https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10/
[night_lights]: https://data.kimetrica.com/dataset/raster-data/resource/07b859ae-cdea-4f7b-99a6-4d48a952f0a6
[Malaria Map Project]: https://map.ox.ac.uk/research-project/accessibility_to_cities/
[Weiss et al. (2018) paper]: https://www.nature.com/articles/nature25181
[pdf version]: http://researchonline.lshtm.ac.uk/4649634/1/weiss_et_al_2017_accepted.pdf
[accessibility surface model]: https://gitlab.kimetrica.com/DARPA/darpa/tree/master/models/accessibility_model
[landcover]: https://data.kimetrica.com/dataset/raster-data/resource/5712155e-5321-4f01-9eab-0de727af53be
[many additional landcover products]: https://archive.usgs.gov/archive/sites/landcover.usgs.gov/landcoverdata.html
[population moduel]: https://gitlab.kimetrica.com/DARPA/darpa/tree/master/models/population_model
[Wave 1]: https://data.kimetrica.com/dataset/south-sudan-high-frequency-survey-2015-wave-1
[Wave 2]: https://data.kimetrica.com/dataset/south-sudan-high-frequency-survey-2016-wave-2
[Wave 3]: https://data.kimetrica.com/dataset/south-sudan-high-frequency-survey-2016-2017-wave-3
[Wave 4]: https://data.kimetrica.com/dataset/south-sudan-high-frequency-survey-2017-wave-4
[Food Consumption Score]: https://inddex.nutrition.tufts.edu/data4diets/indicator/food-consumption-score-fcs
[resource in CKAN]: https://data.kimetrica.com/dataset/household-economic-model-data/resource/cd736b2b-c12e-45e0-80b2-df5f84eafc7b