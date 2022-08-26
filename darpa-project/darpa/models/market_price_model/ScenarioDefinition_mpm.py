import datetime

import luigi
from luigi import Task
from luigi.date_interval import Custom as CustomDateInterval
from luigi.parameter import DateIntervalParameter


# set global params, but don't use luigi.Config because it cannot inherit downstream
class GlobalParametersMPM(Task):

    admin_level_choices = {
        "ADMIN_0": "admin0",
        "ADMIN_1": "admin1",
        "ADMIN_2": "admin2",
        "ADMIN_3": "admin3",
        "ADMIN_4": "admin4",
    }

    admin_level = luigi.ChoiceParameter(
        choices=admin_level_choices.values(),
        default=admin_level_choices["ADMIN_2"],
        description="Select source data at admin level?",
    )

    # Scenario Parameters
    time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2017-01-01"),
            datetime.date.fromisoformat("2017-06-01"),
        ),
        description="The time period for running the models",
    )
    percent_of_normal_rainfall = luigi.FloatParameter(
        default=1.5,
        description="To be deprecated.",
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    rainfall_scenario = luigi.ChoiceParameter(
        choices=["mean", "low", "high", "normal"],
        default="normal",
        description="Normal is actual rainfall from CHIRPS. Low (mean*0.25)and high (2.0*mean)",
    )

    # Variable Parameters
    # percentage change of rainfall
    # always_in_help is a stand in for indicating that the parameter has geo and time
    # attributes attached to it (for the front end to parse)
    rainfall_scenario_time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2017-05-01"),
            datetime.date.fromisoformat("2017-05-02"),
        ),
        description="The time period for the rainfall scenario",
    )
    temperature_scenario = luigi.FloatParameter(
        default=0, description="Temperature perturbation value the unit is in Kelvin"
    )
    temperature_scenario_time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2017-05-01"),
            datetime.date.fromisoformat("2017-05-02"),
        ),
        description="The time period for the temperature scenario",
    )
    return_period_threshold = luigi.IntParameter(
        default=10,
        description="The return period discharge magnitudes used as threshold",
    )

    major_road_speed_offset = luigi.FloatParameter(
        default=0, description="Offset road speed",
    )

    minor_road_speed_offset = luigi.FloatParameter(
        default=0, description="Offset road speed",
    )

    def complete(self):
        print(f"TASK PARAMETERS: {self.__dict__}")
        return True
