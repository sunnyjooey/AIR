# Livestock-model Overview

The livestock model uses gridded climate and soils data to quantify the productivity of a rangeland area to support ruminant livestock populations. The model focuses on the impact of broad management decisions, such as stocking density and duration, on the viability of livestock production, using the metric of diet sufficiency. This metric describes, for each modeled time-step, the extent to which energy intake of the animal diet meets or exceeds maintenance energy needs.

The model consists of two dynamic and interacting submodels: a pasture production submodel and a ruminant diet and physiology submodel. The pasture production submodel is the Century model version 4.6, [Parton et al. 1988](https://doi.org/10.1007/BF02180320); this model uses climate and soils data to predict grass growth. The herbivore submodel simulates diet selection from among the available grass types and estimates whether the selected diet meets or exceeds maintenance energy and protein needs, including pregnancy and lactation energy requirements for females. The herbivore diet and physiology model is adapted from GRAZPLAN, which was developed for ruminant livestock [Freer et al. 2012](https://grazplan.csiro.au/wp-content/uploads/2007/08/TechPaperMay12.pdf).​​
<div align='center'><fig><img src="livestock_model_rpm.png"  width="50%" height="50%"><figcaption>Fig.1. The schematic diagram of livestock model. </figcaption></div>
<br>

## Input datasets
In order to run a crop model the following data are required:
- **Soil data**
    - Fraction sand, silt, clay at 20cm depth (%)
    - Bulk density at 20cm depth (g per cm³)
    - Soil pH at 20cm depth (pH scale)

    Soil data used in the model were derived from [soil property maps of Africa at 250m resolution](https://www.isric.org/projects/soil-property-maps-africa-250-m-resolution).
- **Climate data**
    - Monthly average minimum daily temperature (ºC)
    - Monthly average maximum daily temperature (ºC)
    - Monthly precipitation (cm)

    Climate data used in the model were derived from the [TerraClimate](http://www.climatologylab.org/terraclimate.html) database
- **Forage data**
    - Plant functional type (PFT) cover e.g., C3 grass, C4 grass, arid-shrubland
    - Proportion legume 
    - Management threshold (kg/ha)
    - Animal grazing areas (ha)

    Plant functional type data was derived from [MODIS-MCD12Q1 annual land cover](https://doi.org/10.5067/MODIS/MCD12Q1.006) products while management threshold data was drived from litrature review.
- **Site initial conditions**
    - Site state variables table containing required values initial value for each site state variable
    - Plant functional type for herbaceous plants with their characteristics of C, N, and P in live and standing dead biomass.

    Initial conditions data was drived from litrature review.
- **Livestock herd data** 
    - General breed (_Bos Indicus_ Cattle (Zebus), _Bos Taurus_ Cattle, Sheep, Goat) of grazing animal
    - Sex (non-castrated male, castrated male, breeding female, non-breeding female)
    - Average age (days)
    - Average weight (kg)
    - Weight at birth (kg)
    - Standard reference weight (kg)
    - Stocking density (animals per ha)
    - Conception month for breeding females (month)
    - Calving interval for breeding females (months)
    - Lactation duration for breeding females (months)

    Livestock data used in the model was derived from the Agricultural Sample Survey collected by the [Central Statistical Agency (CSA)](https://www.statsethiopia.gov.et/) of Ethiopia covering the period from 2003/04 to 2018/19. The standard reference weight was drived from from litrature review such as Freer et al. 2012. 

## Output of the model
Livestock model output include:
* Potential Biomass (kg/ha). This is total potential modeled biomass in the absence of grazing, including live and standing dead fractions of all plant functional types. 
```bash
luigi --module models.livestock_model.tasks models.livestock_model.tasks.PotentialBiomass \
--time 2015-01-01-2016-01-01 --local-scheduler --workers 20 --country-level Ethiopia
```

* Standing biomass (kg/ha). This is total modeled biomass after offtake by grazing animals, including live and standing dead fractions of all plant functional types. 
```bash
luigi --module models.livestock_model.tasks models.livestock_model.tasks.StandingBiomass \
--time 2015-01-01-2016-01-01 --local-scheduler --workers 20 --country-level Ethiopia
```

* Animal Density. Distribution of animals inside grazing area polygons. 
```bash
luigi --module models.livestock_model.tasks models.livestock_model.tasks.AnimalDensity \
--time 2015-01-01-2016-01-01 --local-scheduler --workers 20 --country-level Ethiopia
```

* Diet sufficiency. Ratio of metabolizable energy intake to maintenance energy requirements on pixels where animals grazed. 
```bash
luigi --module models.livestock_model.tasks models.livestock_model.tasks.DietSufficiency --time 2015-01-01-2016-01-01 \
--local-scheduler --workers 20 --country-level Ethiopia
```
