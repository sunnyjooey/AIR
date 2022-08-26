## Poverty Model

### Background

In Ethiopia, the Central Statistical Agency's household consumption
expenditure (HCE) survey is conducted every 5 years, taking one full
year to collect nationwide data. The data itself concerns a recall
period of 3 and 12 months, respectively. The purpose of this model is
to explore the feasibility of an additional layer of poverty
assessment, dependent on the groundwork of HCE surveys, but
incorporating data from relevant space-based remote sensing platforms
and model-derived data products in real time. Once fully developed,
this poverty model would provide gridded values for the Poverty
Headcount/Gap Indicator covering the spatio-temporal bounds of the
relevant data, vastly improving on the lag in current poverty
assessment and improving data resolution and coverage.


### Model Structure

The model is a multi-layer perceptron model (a neural network) that
takes the various indicator datasets as input, and outputs a point
estimate for poverty as represented by the Foster-Greer-Thorbecke
(FGT) index. The model is fit to multiple outputs: the classification
of FGT as 0, 1, or in-between, and a regression against the continuous
FGT value (which is zero-inflated and, to a lesser degree,
one-inflated). What was not done, but likely should have been, was
dropping the samples equal to zero or one from the regression output.

### Response Data

The 2010/11 and 2015/16 HCE surveys are the source of ground truth for
a Poverty Headcount/Gap indicator used in model training. The
indicator is called the Foster-Greer-Thorbecke (FGT) index, and
includes a free parameter, $`a`$, used to switch between "headcount" and
"gap" modes. Each survey response provides one sample (subscripted by
$`i`$):

```math
\left\{ y_i =  \left( \frac{ \max(0, z_{s, t} - e_i) }{ z_{s,t} } \right) ^a, w_i \right\}
```

where $`z_{s,t}`$ is a poverty line in the units of $`e_i`$, the total
consumption expenditure per adult equivalent, $`a`$ transitions the
response from binary ($`a=0`$) to continuous ($`a>0`$), and $`w_i`$ is a sample
weight based on population representation. The poverty line is
specific to a space ($`s`$) and time ($`t`$), although no spatial variation in
the poverty line appears in the HCE data. The modeling objective
is to yield an estimator of the expected FGT index,

```math
y_{s,t} \approx \sum_{i \in s,t} y_i w_i.
```

### Predictor Data

The model currently uses four datasets, representative of the types of data
that need different handling within the model fitting and prediction pipelines.

1. LandScan Population: double-duty as weights during aggregation to administrative boundaries
2. ESA CCI Land Cover Classification: a discrete data source that must be one-hot encoded before warping
3. VIIRS Night Lights: single-band
4. Famine Early Warning Systems Net (FEWSNET) Land Data Assimilation System (FLDAS): multi-band

These data are written to FinalTarget as Cloud-Optimized-GeoTIFFs
(COGs) by tasks in the adjacent `utils.cog_tasks` sub-package, which provides
all the details on the data sources.

### Notebooks

- [model demo on 2020-05-04](https://gitlab.com/kimetrica/darpa/notebooks/-/blob/master/storyboards/poverty_model.ipynb) 

### Execution

The model does not rely on the `GlobalParameters` task, but has
parameters with the same functionality. The following example uses all
the parameters available for `models.poverty_model.model.Output`, the terminal model task.

```
import luigi
from models.poverty_model.model import Output

task = Output.from_str_params({
     "train_location": '{"id": "ET", "type": "Feature", "geometry": null, "bbox": [33.0, 3.3, 48.0, 14.9]}',
     "train_period": "2011-01-01-2018-12-32",
     "location": '{"id": "ET", "type": "Feature", "geometry": null, "bbox": [33.0, 3.3, 48.0, 14.9]}',
     "period": "2018-01-01-2018-12-32",
     "admin_level": 1,
     "fgt_a": 1,
     "seed": 1234,
     "threshold": 0.7,
     "predictors": '{"esacci": ["lc"], "landscan": ["lspop"]}',
     "reference_grid": "landscan",
     
})

luigi.build([task])
```

Most runs would rely on several defaults, so a more typical `task` would be:

```
task = Output.from_str_params({
     "location": '{"id": "ET", "type": "Feature", "geometry": null, "bbox": [33.0, 3.3, 48.0, 14.9]}',
     "period": "2018-01-01-2018-12-32",
     "fgt_a": 1,
     "admin_level": 1,
     "threshold": 0.7,     
})
```
