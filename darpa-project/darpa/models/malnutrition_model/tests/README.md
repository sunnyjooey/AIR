# Unit test for Luigi task pipelines
<img src="http://www.saschaheyer.de/content/images/2017/11/test-stress-1.gif"
     alt="stress testing"
     style="float: center; margin-right: 5px;"
     width="30%" height="30%">

## Intro to unit testing

Unit testing is part of a best-practice framework to ensure workflow and pipeline integrity. For this purpose, the Python package `unittest` is used to mock out
an object for testing the code and structure of a luigi task. For more information, refer to the [powerpoint slide](https://docs.google.com/presentation/d/1dx5vmTzwgonLYQWlJLSyHe0UmhK_XL60TQVHOkDhBQ4/edit?ts=5c866b18#slide=id.g101199134a_1_0)
put together by Morris Mwaura.

## How to run a test

To demonstrate the basic elements and structure of the unit test, an example from `malnutrition_model/test_task.py` will be used. Refer to that file for the full test code.

In order to run the test, the following libraries are required for import:

```
import pandas as pd
from io import StringIO
from unittest import mock  # noqa: F401
import luigi

from utils.test_utils import MockTarget, LuigiTestCase  # noqa: F401
from models.malnutrition_model.tasks import (
    VarsStandardized_infer,
    MalnutInference,
)

```

Here, `mock` is imported for mocking out a required `FinalTarget` input to the task under testing. And `LuigiTestCase` sets the backend_class of `IntermediateTargets` and `FinalTargets`
to a `MockTarget` so that the test run is performed in memory, and does not over write production data. In this example, the two tasks being tested,
`VarsStandardized_infer` and `MalnutInference`, are imported as well.

For testing the task `VarsStandardized_infer` that returns an `IntermediateTarget`, a `LuigiTestCase` class is set up like this:

```
class VarsStandardized_inferTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.smart_df2 = [dictionary.data_array.here]

```
`self.smart_df2` is mocking `@requires(MalnutDF_infer)` from the Luigi task, and it refers to the variable, `smart_df2`, assigned to the reading of this input in `malnutrition_model/task.py`.
Note that this is done in dictionary array format because the output of `MalnutDF_infer` is a pickled `IntermediateTarget` object.

After setting up the 'fake' data, define the function `test_output`,

```
    def test_output(self):
        task = VarsStandardized_infer()

        with task.input()[0].open("w") as f:
            f.write(pd.DataFrame(self.smart_df2))

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()

```
Here, `task.input()[0]` is specified with an index because the task `VarsStandardized_infer` actually requires two inputs (`self.smart_df2` refers to the first input). And it's
rendered in DataFrame before running as a task. Given the expected output, the user can now test the core logic. If the same logic is tested on multiple columns, then `subTest` should be used as part of the for-loop, because it has the option to track which column(s), if any, failed the test and provides a clearer error message.

```
# check that there's NO missing values
self.assertFalse(output.isnull().values.any())

# Check expected columns
expected_columns = [
    "NDVI",
    "Population",
    "CPI",
    "med_exp",
    "CHIRPS(mm)_lag3",
    "crop_per_capita",
    "CHIRPS(mm)_lag3_scenario",
    "Apr",
    "Sep",
]

for col in expected_columns:
    with self.subTest(col=col):
        self.assertIn(col, output.columns)

standardized_columns = [
                    "NDVI",
                    "Population",
                    "CPI",
                    "crop_per_capita",
                    "CHIRPS(mm)_lag3",
                    "CHIRPS(mm)_lag3_scenario",
                    "med_exp",
                ]

# check columns are standardized by having some negative values

for col in standardized_columns:
    with self.subTest(col=col):
      self.assertTrue((output[col] < 0).any())

```
For this particular module, the unit test will check that the right column names are present (` self.assertIn("NDVI", output.columns)`), and that there's no missing values. Some of the numerical
columns were normalized within the task, so those columns should reflect the standardization (i.e., negative values). Another thing to check
for is that malnutrition rate is always less than 1.


Sometimes a Luigi task returns a `FinalTarget`, and the user might need to be mindful of the format of the output returned by the unit test.

```
 def test_output(self):
        task = MalnutInference()

        with task.input()[0].open("w") as f:
            f.write(pd.DataFrame(self.norm_df))

        with task.input()[1].open("w") as f:
            f.write(pd.DataFrame(self.smart_df))

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()
        # StringIO returns a pd.df from a string object
        out_df = pd.read_csv(StringIO(output), sep=",")

```
For logic testing, out_df needs to be in dataframe format, and `StringIO` converts a string object to DataFrame. If there're multiple objects returned as the output of a Luigi task,
then the user needs to specify the output by the name given under ` def output(self):`. For example,  `output = task.output()["standard_vars"].open("r").read()`


For a full list of assert methods, please refer to this link [HERE](https://docs.python.org/3/library/unittest.html#unittest.TestCase)
