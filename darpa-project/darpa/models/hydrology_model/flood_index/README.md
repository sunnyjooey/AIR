# Flood Index

The flood index model compute a flash flood indicator that uses a dynamic and distributed runoff co‐efficient to take into account the initial soil moisture. This co‐efficient, namely the European Runoff Index based on Climatology (ERIC), is used to weigh each contribution of the upstream precipitation proportionally to the initial soil moisture. The source of the data is FLDAS. To download the data one need an account with Earth data, the account also need to authorized to access GESDISC data for detailed instructions [see](https://disc.gsfc.nasa.gov/earthdata-login).


## Data
The input data for the flood index include:
* Monthly rainfall flux data is scraped from [FLDAS](https://hydro1.gesdisc.eosdis.nasa.gov/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/)
* Monthly surface runoff data is scraped from [FLDAS](https://hydro1.gesdisc.eosdis.nasa.gov/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/)
* Monthly subsurface runoff data is scraped from [FLDAS](https://hydro1.gesdisc.eosdis.nasa.gov/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/)
* Monthly soil moisture data is scraped from [FLDAS](https://hydro1.gesdisc.eosdis.nasa.gov/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/)
* Digital Elevation data is downloaded from [Hydrosheds](https://hydrosheds.cr.usgs.gov/dataavail.php)
* Sub basin data is downloaded from [Hydrosheds](https://hydrosheds.cr.usgs.gov/dataavail.php)

## Data Preprocessing
The input data are clipped to river basin. The data from FLDAS is resample to match the spatial resolution of the DEM.

## Building the Model
Runoff co-efficient is used to take into account the intial condition of the soil moisture
```math
C_f = \frac{Sr + Subr }{Rf}
```

Where $`C_f`$ is the runoff co-efficient at every grid point, $`Sr`$ is the surface runoff, $`Subr`$ is subsurface runoff, and $`Rf`$ is the rainfall data.

The source of the daily runoff data is [FLDAS](https://hydro1.gesdisc.eosdis.nasa.gov/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/). The soil moisture, rainfall, surface and subsurface data are resampled to the spatial resolution of the Dem. The Dem has a spatial resolution of about 0.000833 degrees. The allow resolution Dem enable accurate calculation of upstream runoff.

The input data for calculating the flood index is masked to river basin. According to Alfieri (2012) the flash flood calculations should be restricted to a catchment of up to 5000 $$km^2$$.

```math
UR (t) = \frac{1}{N}\sum_{i=1}^{N} C_f(t) \times R (t)
```

where $`N`$ is the number of grid cells in the upstream area, $`C_f`$ is the runoff co-efficient, and $`R`$ is the rainfall accumulation.

The  Multiple Flow Direction (MFD) algorithm is used to calculate flow direction, number of upstream cells and flow accumulation.

To extract upstream runoff annual maxima. We calculate monthly upstream runoff for a given year and get the maximum value

Upstream runoff climatology is calculated by getting the average of the 17 upstream runoff annual maximum and is used as reference. The number of years used to calculate the climatology (upstream runoff reference) was dictated by data availability ie. from 2001 to 2018.

```math
ERIC = \frac {UR}{\frac{1}{m}\sum_{j=1}^{m}max(UR)_j}
```

where $`m=17`$ is the number of year used to calculate upstream runoff reference; $`max(UR)_j`$ is the maximum upstream runoff value during year $`j`$


## Output of the model
The flood index output flood areas in Geotiff and GeoJSON format. The task `FloodedAreaGeotiff` output one raster file for each month while the task `FloodedAreaGeoJson` output a single geojson file for the specified time period. The task `GlobalParameters` defined in `utils.scenario_tasks.functions.ScenarioDefinition.py` allow the user to specify area of interest and time period to run the model. Also, the user can choose rainfall scenario and its time period. 

To run Flood index in South Sudan for the period 2017-03-01 to 2017-06-01 with high rainfall for the same period use the command below.
`luigi --module models.hydrology_model.flood_index.tasks models.hydrology_model.flood_index.tasks.FloodedAreaGeoJson --time 2017-03-01-2017-06-01 --rainfall-scenario-time 2017-04-01-2017-06-01 --country-level 'South Sudan' --geography /usr/src/app/models/geography/boundaries/south_sudan_2d.geojson --rainfall-scenario-geography /usr/src/app/models/geography/boundaries/south_sudan_2d.geojson --rainfall-scenario high --local-scheduler`

To run Flood index in Ethiopia for the period 2017-03-01 to 2017-06-01 with high rainfall for the same period use the command below.
`luigi --module models.hydrology_model.flood_index.tasks models.hydrology_model.flood_index.tasks.FloodedAreaGeoJson --time 2017-03-01-2017-06-01 --rainfall-scenario-time 2017-04-01-2017-06-01 --country-level Ethiopia --geography /usr/src/app/models/geography/boundaries/ethiopia_2d.geojson --rainfall-scenario-geography /usr/src/app/models/geography/boundaries/ethiopia_2d.geojson --rainfall-scenario high --local-scheduler`
