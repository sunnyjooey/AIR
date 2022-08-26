import geopandas as gpd
import luigi
import pandas as pd
from kiluigi.targets import FinalTarget
from luigi import Task
from luigi.util import requires

from interventions.interventions.intervention_model import Intervention_provide
from models.malnutrition_model.tasks import MalnutritionInferenceGeoJSON


@requires(MalnutritionInferenceGeoJSON)
class ProvideTypeIntervention(Task):
    provide_type = luigi.ChoiceParameter(
        default="Free food distribution",
        choices=["Free food distribution", "School feeding", "School feeding"],
    )
    num_f_recipients = luigi.IntParameter(
        default=1000, description="Number of recipients per round of intervention"
    )
    num_f_intervention_events = luigi.IntParameter(
        default=20,
        description="Number of intervention events (integer). It needs to correspond to reporting_dur",
    )
    quantity_f_reatment_r_transaction = luigi.FloatParameter(
        default=2,
        description="Quantity of treatment/transaction for one round of intervention",
    )
    indicator = luigi.ChoiceParameter(
        default="gam_rate", choices=["gam_rate", "gam_number"]
    )
    q3_1 = luigi.IntParameter(
        default=2,
        description="The suffix refer to the different items in one unit of treament for one round of intervention (this is for estimating the total cost)",
    )
    q3_2 = luigi.IntParameter(
        default=0,
        description="The suffix refer to the different items in one unit of treament for one round of intervention (this is for estimating the total cost)",
    )
    q3_3 = luigi.IntParameter(
        default=0,
        description="The suffix refer to the different items in one unit of treament for one round of intervention (this is for estimating the total cost)",
    )
    q3_4 = luigi.IntParameter(
        default=0,
        description="The suffix refer to the different items in one unit of treament for one round of intervention (this is for estimating the total cost)",
    )
    normalized_dose_r_treatment = luigi.FloatParameter(
        default=1, description="Normalized dose/treatment delivery efficiency"
    )
    interv_window = luigi.IntParameter(
        default=7, description="Frequency of the intervention (e.g. every 7 days)"
    )
    reporting_dur = luigi.FloatParameter(
        default=150,
        description="Total duration of the intervention period (approximately q2 * interv_window)",
    )

    def output(self):
        return FinalTarget(
            path="interventions/malnutrition_intervention.csv", task=self
        )

    def run(self):
        free_food = Intervention_provide(
            indicator_col=self.indicator,
            admin_level="admin2",
            provide_type=self.provide_type,
            period_col="start",
        )
        maln_target = self.input().path
        impact_effect, impact_cost = free_food.impact_calculation(
            source_fname=maln_target,
            q1=self.num_f_recipients,
            q2=self.num_f_intervention_events,
            q3=self.quantity_f_reatment_r_transaction,
            q3_1=self.q3_1,
            q3_2=self.q3_2,
            q3_3=self.q3_3,
            q3_4=self.q3_4,
            q3_norm=self.normalized_dose_r_treatment,
            interv_window=self.interv_window,
            reporting_dur=self.reporting_dur,
        )
        output_geo = gpd.GeoDataFrame(impact_effect.to_dataframe().reset_index())
        output_geo["rainfall_scenario"] = self.rainfall_scenario
        if self.indicator.endswith("_number"):
            output_geo["intervention"] = output_geo["intervention"].astype(int)

        output_geo["impact"] = output_geo["intervention"] - output_geo["baseline"]
        df = gpd.read_file(maln_target)
        output_geo = output_geo.merge(df[["admin2", "admin1"]], on="admin2", how="left")
        output_geo["cost"] = round(impact_cost, 2)
        output_geo.drop_duplicates(inplace=True)
        # Convert geojson to csv because of Dojo
        out_df = output_geo.drop("geometry", axis=1)
        out_df = pd.DataFrame(out_df)
        out_df["admin0"] = self.country_level
        with self.output().open("w") as out:
            out_df.to_csv(out.name, index=False)
