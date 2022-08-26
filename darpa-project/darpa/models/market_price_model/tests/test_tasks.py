import luigi

import pandas as pd
from pandas import Timestamp

from utils.test_utils import LuigiTestCase

from models.market_price_model.tasks import TrainPriceModel, GroupCommodities
from models.market_price_model.mappings import commodity_group_map


class TrainPriceModelTestCase(LuigiTestCase):
    """
    This test checks that the price model is successfully trained.
    """

    def setUp(self):
        super().setUp()

        self.mock_train_data = {
            "food_Sorghum": pd.DataFrame(
                {
                    "p_food_Sorghum": {
                        (
                            "Upper Nile",
                            Timestamp("2014-11-01 00:00:00"),
                        ): 1.0383064671875,
                        (
                            "Western Equatoria",
                            Timestamp("2017-02-01 00:00:00"),
                        ): 0.7484615692929292,
                        (
                            "Warrap",
                            Timestamp("2014-03-01 00:00:00"),
                        ): 2.7590090266666665,
                        (
                            "Jonglei",
                            Timestamp("2014-12-01 00:00:00"),
                        ): 2.6989247716666664,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 2.7590087,
                        (
                            "Western Equatoria",
                            Timestamp("2016-03-01 00:00:00"),
                        ): 1.2629611566428571,
                        (
                            "Upper Nile",
                            Timestamp("2016-11-01 00:00:00"),
                        ): 1.6410405646666664,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 7.700283464175824,
                        (
                            "Jonglei",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 4.458781290666667,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-07-01 00:00:00"),
                        ): 3.6441672860504197,
                        (
                            "Eastern Equatoria",
                            Timestamp("2014-10-01 00:00:00"),
                        ): 1.5370583947916667,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 2.3873222714376454,
                        ("Upper Nile", Timestamp("2016-04-01 00:00:00")): 1.11797784,
                        ("Upper Nile", Timestamp("2014-03-01 00:00:00")): 1.6786317675,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 2.8679858581512603,
                        (
                            "Upper Nile",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 3.0769230276923074,
                        (
                            "Western Equatoria",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 0.8973522087222221,
                        (
                            "Jonglei",
                            Timestamp("2014-07-01 00:00:00"),
                        ): 1.7921146666666665,
                        (
                            "Central Equatoria",
                            Timestamp("2015-04-01 00:00:00"),
                        ): 2.4032258425000004,
                        (
                            "Upper Nile",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 3.9454093661538456,
                    },
                    "fatalities": {
                        ("Upper Nile", Timestamp("2014-11-01 00:00:00")): 456.0,
                        ("Western Equatoria", Timestamp("2017-02-01 00:00:00")): 18.0,
                        ("Warrap", Timestamp("2014-03-01 00:00:00")): 43.0,
                        ("Jonglei", Timestamp("2014-12-01 00:00:00")): 100.0,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 2.0,
                        ("Western Equatoria", Timestamp("2016-03-01 00:00:00")): 66.0,
                        ("Upper Nile", Timestamp("2016-11-01 00:00:00")): 89.0,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 11.0,
                        ("Jonglei", Timestamp("2015-08-01 00:00:00")): 89.0,
                        ("Eastern Equatoria", Timestamp("2015-07-01 00:00:00")): 0.0,
                        ("Eastern Equatoria", Timestamp("2014-10-01 00:00:00")): 0.0,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 20.0,
                        ("Upper Nile", Timestamp("2016-04-01 00:00:00")): 55.0,
                        ("Upper Nile", Timestamp("2014-03-01 00:00:00")): 166.0,
                        ("Eastern Equatoria", Timestamp("2015-06-01 00:00:00")): 6.0,
                        ("Upper Nile", Timestamp("2015-06-01 00:00:00")): 87.0,
                        ("Western Equatoria", Timestamp("2016-04-01 00:00:00")): 5.0,
                        ("Jonglei", Timestamp("2014-07-01 00:00:00")): 0.0,
                        ("Central Equatoria", Timestamp("2015-04-01 00:00:00")): 23.0,
                        ("Upper Nile", Timestamp("2015-08-01 00:00:00")): 24.0,
                    },
                    "crop_prod": {
                        ("Upper Nile", Timestamp("2014-11-01 00:00:00")): 19272.0,
                        (
                            "Western Equatoria",
                            Timestamp("2017-02-01 00:00:00"),
                        ): 110318.0,
                        ("Warrap", Timestamp("2014-03-01 00:00:00")): 127449.0,
                        ("Jonglei", Timestamp("2014-12-01 00:00:00")): 23107.0,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 74951.0,
                        (
                            "Western Equatoria",
                            Timestamp("2016-03-01 00:00:00"),
                        ): 116936.0,
                        ("Upper Nile", Timestamp("2016-11-01 00:00:00")): 29613.0,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 91264.0,
                        ("Jonglei", Timestamp("2015-08-01 00:00:00")): 31826.0,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-07-01 00:00:00"),
                        ): 117580.0,
                        (
                            "Eastern Equatoria",
                            Timestamp("2014-10-01 00:00:00"),
                        ): 142146.0,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 106209.0,
                        ("Upper Nile", Timestamp("2016-04-01 00:00:00")): 29613.0,
                        ("Upper Nile", Timestamp("2014-03-01 00:00:00")): 19272.0,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 117580.0,
                        ("Upper Nile", Timestamp("2015-06-01 00:00:00")): 26851.0,
                        (
                            "Western Equatoria",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 116936.0,
                        ("Jonglei", Timestamp("2014-07-01 00:00:00")): 23107.0,
                        (
                            "Central Equatoria",
                            Timestamp("2015-04-01 00:00:00"),
                        ): 217504.0,
                        ("Upper Nile", Timestamp("2015-08-01 00:00:00")): 26851.0,
                    },
                    "rainfall": {
                        (
                            "Upper Nile",
                            Timestamp("2014-11-01 00:00:00"),
                        ): 49.871639251708984,
                        (
                            "Western Equatoria",
                            Timestamp("2017-02-01 00:00:00"),
                        ): 2.127845048904419,
                        (
                            "Warrap",
                            Timestamp("2014-03-01 00:00:00"),
                        ): 0.6867466568946838,
                        (
                            "Jonglei",
                            Timestamp("2014-12-01 00:00:00"),
                        ): 20.462688446044922,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 49.85752487182617,
                        (
                            "Western Equatoria",
                            Timestamp("2016-03-01 00:00:00"),
                        ): 3.32808518409729,
                        (
                            "Upper Nile",
                            Timestamp("2016-11-01 00:00:00"),
                        ): 37.112388610839844,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 59.985660552978516,
                        (
                            "Jonglei",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 73.39254760742188,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-07-01 00:00:00"),
                        ): 64.39183807373047,
                        (
                            "Eastern Equatoria",
                            Timestamp("2014-10-01 00:00:00"),
                        ): 43.37302017211914,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 56.89631271362305,
                        (
                            "Upper Nile",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 2.577465057373047,
                        (
                            "Upper Nile",
                            Timestamp("2014-03-01 00:00:00"),
                        ): 0.4238566756248474,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 66.49031066894531,
                        (
                            "Upper Nile",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 46.311397552490234,
                        (
                            "Western Equatoria",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 70.84496307373047,
                        (
                            "Jonglei",
                            Timestamp("2014-07-01 00:00:00"),
                        ): 62.05739974975586,
                        (
                            "Central Equatoria",
                            Timestamp("2015-04-01 00:00:00"),
                        ): 27.110862731933594,
                        (
                            "Upper Nile",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 56.18354415893555,
                    },
                    "p_petrol": {
                        (
                            "Upper Nile",
                            Timestamp("2014-11-01 00:00:00"),
                        ): 5.591397933333333,
                        (
                            "Western Equatoria",
                            Timestamp("2017-02-01 00:00:00"),
                        ): 164.386869192,
                        (
                            "Warrap",
                            Timestamp("2014-03-01 00:00:00"),
                        ): 4.3355856133333335,
                        ("Jonglei", Timestamp("2014-12-01 00:00:00")): 4.032258125,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 2.069256525,
                        (
                            "Western Equatoria",
                            Timestamp("2016-03-01 00:00:00"),
                        ): 0.1829976125,
                        ("Upper Nile", Timestamp("2016-11-01 00:00:00")): 0.0,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 3.10580116,
                        ("Jonglei", Timestamp("2015-08-01 00:00:00")): 4.4354838,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-07-01 00:00:00"),
                        ): 4.7394540184615375,
                        (
                            "Eastern Equatoria",
                            Timestamp("2014-10-01 00:00:00"),
                        ): 2.307687692307692,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 0.3454862075,
                        ("Upper Nile", Timestamp("2016-04-01 00:00:00")): 0.4631988,
                        ("Upper Nile", Timestamp("2014-03-01 00:00:00")): 0.67567568,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 1.9602977961538461,
                        ("Upper Nile", Timestamp("2015-06-01 00:00:00")): 6.4516128,
                        (
                            "Western Equatoria",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 0.052109865,
                        ("Jonglei", Timestamp("2014-07-01 00:00:00")): 2.5338,
                        (
                            "Central Equatoria",
                            Timestamp("2015-04-01 00:00:00"),
                        ): 2.762096815625,
                        (
                            "Upper Nile",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 6.820276388571428,
                    },
                    "p_food_Sorghum_t-1": {
                        (
                            "Upper Nile",
                            Timestamp("2014-11-01 00:00:00"),
                        ): 1.2701613093750002,
                        (
                            "Western Equatoria",
                            Timestamp("2017-02-01 00:00:00"),
                        ): 0.7751402139393938,
                        (
                            "Warrap",
                            Timestamp("2014-03-01 00:00:00"),
                        ): 2.9279279466666663,
                        (
                            "Jonglei",
                            Timestamp("2014-12-01 00:00:00"),
                        ): 1.838709705000001,
                        ("Lakes", Timestamp("2013-11-01 00:00:00")): 2.4774772,
                        (
                            "Western Equatoria",
                            Timestamp("2016-03-01 00:00:00"),
                        ): 1.2941847573015872,
                        ("Upper Nile", Timestamp("2016-11-01 00:00:00")): 1.085756672,
                        ("Lakes", Timestamp("2015-10-01 00:00:00")): 6.379739027106228,
                        (
                            "Jonglei",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 4.293906741333333,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-07-01 00:00:00"),
                        ): 2.8679858581512603,
                        ("Eastern Equatoria", Timestamp("2014-10-01 00:00:00")): 1.6129,
                        ("Lakes", Timestamp("2016-05-01 00:00:00")): 2.148921767516608,
                        (
                            "Upper Nile",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 1.6970000651538464,
                        ("Upper Nile", Timestamp("2014-03-01 00:00:00")): 2.259290555,
                        (
                            "Eastern Equatoria",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 2.53004431372549,
                        (
                            "Upper Nile",
                            Timestamp("2015-06-01 00:00:00"),
                        ): 3.2878411384615385,
                        (
                            "Western Equatoria",
                            Timestamp("2016-04-01 00:00:00"),
                        ): 1.2629611566428571,
                        (
                            "Jonglei",
                            Timestamp("2014-07-01 00:00:00"),
                        ): 1.6141244444444443,
                        (
                            "Central Equatoria",
                            Timestamp("2015-04-01 00:00:00"),
                        ): 2.032258095,
                        (
                            "Upper Nile",
                            Timestamp("2015-08-01 00:00:00"),
                        ): 3.8337468369230767,
                    },
                }
            )
        }

    def test_output(self):
        task = TrainPriceModel()

        with task.input().open("w") as dst:
            dst.write(self.mock_train_data)

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()["food_Sorghum"]
        params = output.fit().params

        # make sure we have all the expected variables in the trained model
        expected_cols = [
            "const",
            "crop_prod",
            "fatalities",
            "p_petrol",
            "rainfall",
            "p_food_Sorghum_t-1",
        ]

        for col in expected_cols:
            with self.subTest(col=col):
                self.assertTrue(col in params.index)
        # lagged price of food should be positively correlated
        self.assertTrue(params["p_food_Sorghum_t-1"] > 0)


class GroupCommoditiesTestCase(LuigiTestCase):
    """
    This test checks that individual commodities are properly grouped.
    """

    def setUp(self):
        super().setUp()

        self.mock_predictions = {
            "food_Groundnuts": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 0.8481310168885298,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.1915474427793677,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.4712489887138454,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Sorghum": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 0.8591689448408244,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.0968785868275397,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.2651231706045845,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Wheat": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 1.4026914912375912,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.689362728496268,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.936955722899594,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Okra": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 1.3982474010623007,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.7335075891214422,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 2.2161272422724827,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Rice": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 1.2349173824714037,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.6397775266482653,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.8790454352298398,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Veg Oil": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 2.04679935870177,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 2.381398833318018,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 2.7748779305396707,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Sugar": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 1.32336192138305,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.7150795754409898,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.9847093786905672,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Milk": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 0.8483040075960373,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 0.9718009734955252,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.1567765345317795,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Beans": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 1.4185418985332674,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.9312466413076257,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 2.373082819129927,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Meat": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 2.8761986745087555,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 3.3179675806960325,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 3.769588425704823,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
            "food_Maize": {
                "predictions": {
                    (
                        "Central Equatoria",
                        Timestamp("2017-04-01 00:00:00"),
                    ): 0.8892347642127693,
                    (
                        "Central Equatoria",
                        Timestamp("2017-05-01 00:00:00"),
                    ): 1.1565143022170599,
                    (
                        "Central Equatoria",
                        Timestamp("2017-06-01 00:00:00"),
                    ): 1.3585819073096588,
                },
                "geometry": {
                    ("Central Equatoria", Timestamp("2017-04-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-05-01 00:00:00")): None,
                    ("Central Equatoria", Timestamp("2017-06-01 00:00:00")): None,
                },
            },
        }

    def test_output(self):
        task = GroupCommodities()

        with task.input().open("w") as dst:
            dst.write(self.mock_predictions)

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)

        with task.output().open("r") as src:
            output = src.read()

        for col in output.columns[output.columns != "geometry"]:
            with self.subTest(col=col):
                # check that commodity columns are properly named
                self.assertTrue(col in commodity_group_map.values())
                # check that prices for commodity groups are non-negative
                self.assertTrue((output[col] >= 0).all())
