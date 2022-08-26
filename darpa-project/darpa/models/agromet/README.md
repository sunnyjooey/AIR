# Meteorological and Agricultural drought Indices

Calculate meteorological, hydrological, and agricultural drought indices.

## Input data
The data used to cacluate the indices include:
* Monthly rainfall obtained from [CHIRPS FTP](ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/africa_monthly/tifs/)
* Monthly potential evapotranspiration obtained from [TerraClimate](http://thredds.northwestknowledge.net:8080/thredds/catalog/TERRACLIMATE_ALL/data/catalog.html)
* Available water capacity obtained from [ISRIC](https://data.isric.org/geonetwork/srv/eng/catalog.search;jsessionid=1A77633FFF6B3F4D39EC2C6AF2684275#/metadata/10aa9aa3-1433-11e9-a8fa-a0481ca9e724)

## Indices
The available indices area:
* Standardized Precipitation Index [SPI](https://climatedataguide.ucar.edu/climate-data/standardized-precipitation-index-spi)
* Standardized Precipitation-Evapotranspiration Index [SPEI](https://www.researchgate.net/publication/252361460_The_Standardized_Precipitation-Evapotranspiration_Index_SPEI_a_multiscalar_drought_index)
* Self-calibrated Palmer Drought Severity Index [scPDSI](https://www.droughtmanagement.info/self-calibrated-palmer-drought-severity-index-sc-pdsi/) 
* Palmer Drought Severity Index [PDSI](https://www.droughtmanagement.info/palmer-drought-severity-index-pdsi/)
* Palmer Hydrological Drought Index [PHDI](https://www.droughtmanagement.info/palmer-hydrological-drought-index-phdi/)
* Modified Palmer Drought Severity Index [PMDI](https://climate.ncsu.edu/climate/climdiv)
* Palmer moisture anomaly index [Z-index](https://www.droughtmanagement.info/palmer-z-index/)

## Output data
The repository output indices in both GeoJSON and GeoTIFF format:
* Raster file with actual value of the indices eg the task `StandardizedPrecipitationIndex`
`luigi --module models.agromet.tasks models.agromet.tasks.StandardizedPrecipitationIndex --country-level Ethiopia --time 2017-01-01-2017-03-01 --geography /usr/src/app/models/geography/boundaries/ethiopia_2d.geojson --workers 20 --local-scheduler`
* Indices aggregated at various admin level eg the task `ConvertStandardizedPrecipitationIndexToGeojson`
`luigi --module models.agromet.tasks models.agromet.tasks.ConvertStandardizedPrecipitationIndexToGeojson --country-level Ethiopia --time 2017-01-01-2017-03-01 --geography /usr/src/app/models/geography/boundaries/ethiopia_2d.geojson --workers 20 --local-scheduler`
