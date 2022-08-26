# Soil Moisture model
The soil moiture model is composed of two models:
* Downscaling the soil moisture data from 9km to 2.25 km
* Predicting soil moisture model


## Dowscaling Remotely Sensed Soil Moisture

Soil moisture Active passive (SMAP) soil moisture data was used to map [agricultural flood in Louisiana](https://www.researchgate.net/publication/319977521_Agriculture_flood_mapping_with_Soil_Moisture_Active_Passive_SMAP_data_A_case_of_2016_Louisiana_flood). The overall accuracy was 60% and about 32% of commission error. The over estimation of the flood by SMAP data was due to the coarse spatial resolution (9km) of the SMAP data. Soil moisture model use neural network based downscaling algorithm to disaggregate soil moisture to 2.25 km spatial resolution.

Our downscaling algorithm is based on an NN approach that uses the two soil moisture estimates from SMAP at 36 and
9 km for training, and then retrieves soil moisture at 2.25 km spatial resolution using the 9 km estimates from SMAP. The NN relates the coarse-scale soil moisture and NDVI estimates as well as the fine-scale NDVI estimates as input to the fine-scale soil moisture estimates as output. To do so, we assume that the scaling relationship between 36 and 9 km soil moisture estimates is the same as the scaling relationship between 9 and 2.25 km estimates.

### Dataset
#### Soil Moisture
The pipeline scrape [SMAP 36km Soil moisture data](https://n5eil01u.ecs.nsidc.org/SMAP/SPL3SMP.005/) and [SMAP 9km soil moisture data](https://n5eil01u.ecs.nsidc.org/SMAP/SPL3SMP_E.002/).

#### NDVI
The pipeline scrape [Monthly averaged NDVI data](https://e4ftl01.cr.usgs.gov/MOLT/MOD13A3.006/). To specify the area of interest the user provide a list of tiles to be downloaded. A method for getting tiles from bounding box can be added but this task will change once we move the model to [descartes labs](https://www.descarteslabs.com/).

### Data Preprocessing

To generate the coarse-scale soil moisture estimates that are used as input to the NN algorithm, we use a moving window averaging at the coarser resolution grid (as shown in the figure below) that is centered on the target pixel at the finer grid. In the training step, this moving window is 45 km (applied over the 36 km product) and in the retrieval step, the moving window is 11.25 km (applied over the 9 km product). All the inputs to the algorithm are averaged at the scale of the moving window from the coarse-resolution grid using an area-weighted averaging. The only disadvantage of the moving window technique is that values at the edges are not valid.

<img src="https://gitlab.kimetrica.com/DARPA/darpa/raw/13287-Masking-Geography-accessibility-hydrology/models/hydrology_model/soil_moisture/data/input_data.png"
     alt="stress testing"
     style="float: center;"
     width="40%" height="40%">

### Downscaling Model

We train the model using data from 2015 and 2016, sampling the data using every 10 days. The soil moisture data for 2017 is used to test the model.

The final MLP has one three layers (input, hidden, and output). The input layer has three neurons, hidden layers has five while the output layer has one. We use the rectified linear unit as the activation function for the hidden layer and linear function as the activation function for the output layer.

The inputs include:
* Soil moisture estimates from SMAP on the moving window grid at the coarse-scale resolution (45 km for training and 11.25 km for retrieval)
*  Mean NDVI at the coarse-scale moving window and NDVI in the target pixel at the fine-scale grid (9 km for training and 2.25 km for retrieval).


## Soil Moisture Prediction Model

The model predict soil moisture using rainfall for the last 10 days and Geomorphological features.

### Dataset

#### Rainfall data
The rainfall data for the last 10 days. [CHIRPS](ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/africa_daily/tifs/p05/) is the source the daily rainfall data

#### LandForms
[ALOS](https://developers.google.com/earth-engine/datasets/catalog/CSP_ERGo_1_0_US_landforms) is the souce of the land form data is

#### Soil texture
[ISRIC](https://www.isric.org/projects/soil-property-maps-africa-250-m-resolution) is the source of the soil texture data is

#### Topographic wetness index
Topographic wetness index is calculated from slope and catchment area

## output of the model
* Soil Moisture
* Impassable areas

