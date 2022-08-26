import luigi
from luigi import Task


class CountryParameters(Task):

    country_choices = {
        "SS": "South Sudan",
        "ETH": "Ethiopia",
        "DJ": "Djibouti",
        "UG": "Uganda",
        "KE": "Kenya",
        "SD": "Sudan",
        "SO": "Somalia",
        "ER": "Eritrea",
    }

    country_level = luigi.ChoiceParameter(
        choices=country_choices.values(),
        default=country_choices["SS"],
        description="Select source data at country level?",
    )

    def complete(self):
        print(f"TASK PARAMETERS: {self.__dict__}")
        return True
