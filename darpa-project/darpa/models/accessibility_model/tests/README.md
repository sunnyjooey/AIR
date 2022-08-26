## Unit test for Accessibility Model

The unit test help the developer/analyst find bug before the models are deployed.
This documentation will focus on testing a task's method that returns a  numpy array
and a task that expect a raster as input and output a raster files.
Please see the documnetion by [Moriss](https://docs.google.com/presentation/d/1dx5vmTzwgonLYQWlJLSyHe0UmhK_XL60TQVHOkDhBQ4/edit?ts=5c866b18#slide=id.g101199134a_1_0)
and [Jenny](https://gitlab.kimetrica.com/DARPA/darpa/blob/master/models/malnutrition_model/tests/README.md)

### Testing Task's Method
Create a class by subclassing `LuigiTestCase`. Inside the `setUP` method, create fake
input data for the method to be tested.

```
class FrictionSurfaceTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        # Create fake input data
        self.landspeed = np.linspace(1, 5, 25).reshape(5, 5)
        self.landspeed[0, 0] = -9999.0
        self.dem = np.linspace(1000, 2500, 25).reshape(5, 5)
        self.dem[4, 4] = -9999.0
        self.slope = np.radians(np.linspace(0, 85, 25).reshape(5, 5))
        self.slope[4, 4] = -9999.0
```

Create a method with name `test_`  concate with the name of the method to be tested.
Compute the expected result manually and use `np.testing.assert_array_almost_equal`
to compare the method's output and the expected array.

```
    def test_adjust_landspeed_for_elevation(self):
        # Expected land speed adjusted for elevation
        expected_adj_speed = np.array(
            [
                -9999.0,
                1.0577,
                1.2008,
                1.3418,
                1.481,
                1.6182,
                1.7535,
                1.887,
                2.0185,
                2.1483,
                2.2762,
                2.4023,
                2.5266,
                2.6492,
                2.77,
                2.8891,
                3.0064,
                3.1221,
                3.2361,
                3.3484,
                3.4591,
                3.5682,
                3.6756,
                3.7815,
                -9999.0,
            ]
        ).reshape(5, 5)
        computed_adj_speed = FrictionSurface().adjust_landspeed_for_elevation(
            self.landspeed, -9999.0, self.dem, -9999.0
        )
        np.testing.assert_array_almost_equal(computed_adj_speed, expected_adj_speed, 4)
```

### Testing a task that expect a raster and output a raster

For this case we will be using `TravelTimeToTownsTest` as an example.
Within the `setUp` method we create fake numpy array and meta-data to be used to
create a fake rasters.

```
class TravelTimeToTownsTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        # Create input frictions
        self.friction_array = np.array([4, 10, 5, 7, 8, 1, 6, 2, 9]).reshape(3, 3)
        self.friction_array = self.friction_array.astype(rasterio.ubyte)

        # Create destination array
        self.dest_array = np.zeros((3, 3))
        self.dest_array[0, 0] = 1
        self.dest_array[1, 2] = 1
        self.dest_array = self.dest_array.astype(rasterio.ubyte)

        # Create destination geodataframe
        self.dest_df = pd.DataFrame(
            {"coordinates": ["Point(24.225 12.225)", "Point(24.175 12.125)"]}
        )
        self.dest_df["coordinates"] = self.dest_df["coordinates"].apply(wkt.loads)
        self.dest_df = gpd.GeoDataFrame(self.dest_df, geometry="coordinates")

        # Rater metadata
        self.raster_metadata = {
            "transform": affine.Affine(0.05, 0.0, 24.15, 0.0, -0.05, 12.25),
            "driver": "GTiff",
            "width": 3,
            "height": 3,
            "count": 1,
            "crs": CRS.from_epsg(4326),
            "dtype": rasterio.ubyte,
        }
```

The task `TravelTimeToTowns` `requires` a list of two inputs. The first input,
outputs a dictionary of targets and we are only interested in one of them with
the key `towns_rast`. Thus we will open and close all the targets except `towns_rast`.
```
def test_travel_time_to_area_destination(self):
        task = TravelTimeToTowns()
        for key in task.input()[0]:
            if key != "towns_rast":
                f = task.input()[0][key].open("w")
                f.close()

        with task.input()[0]["towns_rast"].open("w") as f:
            with rasterio.open(f, "w", **self.raster_metadata) as dst:
                dst.write(self.dest_array, 1)

        with task.input()[1].open("w") as f:
            with rasterio.open(f, "w", **self.raster_metadata) as out_raster:
                out_raster.write(self.friction_array, 1)

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)

        with rasterio.open(task.output()["travel_time"].open()) as src:
            actual_output = np.squeeze(src.read())

        # Expected output
        expected_output = np.array(
            [2000.0, 7000.0, 3000.0, 5500, 4500, 500, 6121.3203, 2121.3203, 5000.0]
        ).reshape(3, 3)

        np.testing.assert_array_almost_equal(expected_output, actual_output, 4)
```

