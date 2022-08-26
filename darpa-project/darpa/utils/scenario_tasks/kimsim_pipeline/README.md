
# KimSim Pipeline

This pipeline was created to feed data to the KimSim simulation environment, currently being developed by Bolder Games, to dynamically generate a scenario with high fidelity to the physical and socioeconomic world. The pipeline currently relies on data from static sources, stored in CKAN, dynamically scraped climate variables, and models developed by Kimetrica. Where possible, the pipeline returns data specific to a user-specified time and geography. The pipeline currently returns the following values:

- scenario start and end date
- total population within the scenario geography, as well as population broken down by age and sex (time-static)
- birth and death rates for the scenario time and geography
- minimum, maximum, median, and standard deviation of HH consumption expenditure, as well as % of population below poverty line for scenario geography (time-static)
- daily rainfall for scenario time and geography
- building locations for scenario geography (time-static)
- crop locations for scenario time and geography

The pipeline is triggered by calling the `GetKimSimBaseData` task with user-defined `time` and `geography` parameters, and returns a dictionary in the following format (requested by BG):

`{'SimulationStart': datetime.datetime(2017, 3, 1, 0, 0), 
 'SimulationEnd': datetime.datetime(2017, 6, 1, 0, 0), 
 'population_total_initial': 36, 
 'VillageAdultMalePopulation': 11.0, 
 'VillageAdultFemalePopulation': 8.0, 
 'VillageChildrenPopulation': 17.0, 
 'birth_rate_p_y': 0.05416607, 
 'death_rate_p_y': 0.016553085, 
 'HouseholdIncomeMin': 2.1478202, 
 'HouseholdIncomeMax': 17.537155, 
 'HouseholdIncomeMedian': 5.2447705, 
 'HouseholdIncomeStdDev': 2.1586187, 
 'poverty_line_percent_below': 0.40659884, 
 'rainfall': [0.32908797, 0.0, 0.0, 0.0, 0.0, 0.0, 0.01532193, 0.0, 0.0, 0.16722316,
              0.0, 0.0, 0.0, 0.0, 0.29751825, 1.9465652, 0.4378936, 0.89861494, 0.0, 
              3.7573762, 0.0, 0.0, 0.0120299645, 0.0, 1.4515885, 0.25921732, 0.08060163,
              4.8732376, 0.0, 0.045690004, 0.0, 0.14438625, 0.0, 0.04661663, 0.5995879, 
              0.0, 1.0785605, 0.0, 0.18965243, 0.0, 0.0, 0.05632021, 0.0, 0.0, 2.1069224, 
              0.35625145, 0.088527285, 4.1822987, 0.03156014, 0.8240492, 0.0, 0.663792, 
              2.029234, 3.6977782, 5.84268, 9.678688, 2.60754, 2.8084824, 0.7214382, 
              6.501595, 3.7618651, 0.0, 3.0610783, 4.5201488, 0.16716021, 2.6275523, 
              2.3948078, 1.5306547, 6.734303, 1.1434442, 0.0, 0.0, 2.3610642, 3.5269644,
              0.4609513, 4.004176, 0.06492618, 0.75517714, 3.9419644, 1.8553836, 0.5026674,
              0.0, 0.35182956, 4.3667836, 0.0, 0.8947889, 0.0, 5.438334, 3.138798, 2.9059815,
              1.0607685, 1.0177457, 0.29608938], 
 'buildings': [(3.7746838244299794, 31.638370596071574), (3.7390967, 31.683926400000004),
               (3.7237193, 31.658081150000005), (3.774809626542392, 31.638500920110012), 
               (3.73755225, 31.682985799999997), (3.768899429529935, 31.63486427628819), 
               (3.756140075662354, 31.65626017301294), (3.7236725389044842, 31.6584570281368), 
               (3.756900800944655, 31.656550565752266), (3.769844582728297, 31.635387899493157), 
               (3.756030175954959, 31.656266771793263), (3.7572298, 31.656394), 
               (3.7698584242092354, 31.63491259549591),...],
 'fields': [(4.325636190666243, 31.15755518054799), (4.3258888876648, 31.15747395651274),
            (4.325717414701494, 31.157853002010576), (4.32603328594969, 31.15797032561705),
            (4.325906937450412, 31.15763640458324), (4.325906937450412, 31.157943250938633),
            (4.325536916845381, 31.15755518054799), (4.3255549666309925, 31.15749200629835),
            (4.325970111700051, 31.157528105869574),(4.325708389808688, 31.157816902439354), ...]
}
`

## Moving Forward

The complete list of data and parameters requested by BG can be found [here](https://docs.google.com/spreadsheets/d/1THHolkzPKCdeTN4yi4UMGHQhFSLry6DwQDOD1Lj2rig/edit?usp=sharing). Every task added to the pipeline to produce one of these values should output a dictionary of the format `{parameter name: value or list of values}`, even if the task only produces one quantity, as this allows us to consolidation the parameters into a single data structure most easily. 

There may also need to be some restructuring of the pipeline once our data store is up and running, as data and model results are currently being pulled from CKAN.


```python

```
