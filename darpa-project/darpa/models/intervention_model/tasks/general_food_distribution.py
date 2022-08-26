import json

import luigi
import numpy as np
import rasterio
from luigi.configuration import get_config as luigi_config
from luigi.util import requires
from rasterio.warp import Resampling, reproject

from kiluigi.targets import IntermediateTarget

from models.demand_model.tasks import CreateCaloricNeedsSurface
from models.population_model.tasks import CreateHighResPopulationEstimateRaster


CACHE_DIR = luigi_config().get("core", "cache_dir")


@requires(CreateCaloricNeedsSurface, CreateHighResPopulationEstimateRaster)
class CalculateMeanCaloricNeed(luigi.Task):
    """
    Calculate a weighted mean caloric need over the whole region.
    """

    def output(self):
        return IntermediateTarget(
            f"caloric_needs_stats_{self.caloric_threshold}_{self.year}.txt", task=self
        )

    def run(self):
        with rasterio.open(self.input()[0].path, "r") as src_needs_rast:
            with rasterio.open(self.input()[1].path, "r") as pop_rast:

                assert src_needs_rast.bounds == pop_rast.bounds
                assert src_needs_rast.crs == pop_rast.crs

                dst_needs_array = np.zeros(pop_rast.shape, src_needs_rast.meta["dtype"])
                reproject(
                    source=src_needs_rast.read(1),
                    destination=dst_needs_array,
                    src_transform=src_needs_rast.transform,
                    src_crs=src_needs_rast.crs,
                    dst_transform=pop_rast.transform,
                    dst_crs=pop_rast.crs,
                    resampling=Resampling.nearest,
                )

                caloric_needs_array = dst_needs_array

                population_array = pop_rast.read(1)

                assert caloric_needs_array.shape == population_array.shape

                # set nodatavalue
                population_array[population_array == pop_rast.nodata] = 0
                population_array[population_array == np.nan] == 0
                caloric_needs_array[caloric_needs_array == src_needs_rast.nodata] = 0
                caloric_needs_array[caloric_needs_array == np.nan] = 0

                # Set the caloric deficit value to zero in all the cells where
                # it's positive.
                caloric_needs_array[caloric_needs_array > 0] = 0

                # Calculate weighted mean caloric deficit over the whole region.
                # In doing so, mask the population in all the cells where the
                # corresponding caloric deficit value is zero in order to prevent
                # those cells from contributing to the weighing.
                mask = np.ma.masked_equal(caloric_needs_array, 0).mask
                masked_population_array = np.ma.masked_array(population_array, mask)

                mean_caloric_need = abs(
                    np.sum(caloric_needs_array * masked_population_array)
                    / np.sum(masked_population_array)
                )

                # calculate the poverty gap index (or more correctly the hunger gap index).
                total_population = np.sum(population_array)
                poverty_gap_index = abs(
                    np.sum(
                        caloric_needs_array
                        * masked_population_array
                        / self.caloric_threshold
                    )
                    / total_population
                )

        caloric_needs_stats = {
            "mean_caloric_need": float(mean_caloric_need),
            "population_in_need": float(np.sum(masked_population_array)),
            "total_population": float(total_population),
            "poverty_gap_index": float(poverty_gap_index),
        }

        with open(self.output().path, "w") as jsonfile:
            jsonfile.write(json.dumps(caloric_needs_stats))


@requires(CalculateMeanCaloricNeed)
class CalculateTotalRations(luigi.Task):
    """

    Parameters
    -----------
    food_basket: dict
        The food basket is dictionary of ration items, their contribution in
        calories (kcal) and a conversion_factor to convert from calories to
        quantity (in grams). The conversion factor is expressed in form of
        the total calories per 100g of the ration item.
    """

    default_food_basket = {
        "maize": {"calories_per_100g": 365, "contrib": 0.65},
        "beans": {"calories_per_100g": 337, "contrib": 0.15},
        "vegetable_oil": {"calories_per_100g": 862, "contrib": 0.20},
    }

    food_basket = luigi.DictParameter(default=default_food_basket)
    duration = luigi.IntParameter(default=1)

    def output(self):
        return IntermediateTarget(
            f"total_rations_{self.caloric_threshold}_{self.duration}_{self.year}.txt",
            task=self,
        )

    def run(self):
        self.validate_food_basket()

        with open(self.input().path) as inputfile:
            caloric_needs_stats = json.loads(inputfile.read())

        conversion_factor = 0.1  # To convert 100g to Kg
        total_rations = {}
        for ration in self.food_basket:
            calories = (
                self.food_basket[ration]["contrib"]
                * caloric_needs_stats["mean_caloric_need"]
            )
            daily_ration_per_person = conversion_factor * (
                calories / self.food_basket[ration]["calories_per_100g"]
            )
            daily_ration_per_population = (
                daily_ration_per_person * caloric_needs_stats["population_in_need"]
            )
            total_ration = daily_ration_per_population * self.duration

            total_rations[ration] = {
                "daily_ration_per_person": daily_ration_per_person,
                "daily_ration_per_population": daily_ration_per_population,
                "total_ration": total_ration,
            }

        with open(self.output().path, "w") as jsonfile:
            jsonfile.write(json.dumps(total_rations))

    def validate_food_basket(self):
        # Validate that the calorie contribution adds up to 1 (100%)
        total_contrib = 0
        for ration in self.food_basket:
            total_contrib += self.food_basket[ration]["contrib"]

        assert total_contrib == 1
