import geopandas as gpd
import luigi
import pandas as pd
from kiluigi.targets import FinalTarget
from luigi import Task
from luigi.util import requires

from interventions.interventions.intervention_model import Intervention_provide
from models.measles_model.tasks import PredictionGeojson


@requires(PredictionGeojson)
class MeaslesInterventionImpactCost(Task):
    num_f_recipients = luigi.IntParameter(
        default=50, description="Number of recipients per round of intervention"
    )
    num_f_intervention_events = luigi.IntParameter(
        default=4,
        description="Number of intervention events (integer). It needs to correspond to reporting_dur",
    )
    quantity_f_reatment_r_transaction = luigi.FloatParameter(
        default=2,
        description="Quantity of treatment/transaction for one round of intervention",
    )
    q3_1 = luigi.IntParameter(
        default=1,
        description="The suffix refer to the different items in one unit of treament for one round of intervention (this is for estimating the total cost)",
    )
    normalized_dose_r_treatment = luigi.FloatParameter(
        default=1, description="Normalized dose/treatment delivery efficiency"
    )
    interv_window = luigi.IntParameter(
        default=30, description="Frequency of the intervention (e.g. every 7 days)"
    )
    reporting_dur = luigi.FloatParameter(
        default=30 * 4,
        description="Total duration of the intervention period (approximately q2 * interv_window)",
    )

    def output(self):
        return FinalTarget(path="interventions/measles_intervention.csv", task=self)

    def run(self):
        measles_intervention = Intervention_provide(
            indicator_col="measles_number",
            provide_type="vaccine",
            admin_level="admin1",
            period_col="timestamp",
        )

        measles_target = self.input().path
        intervention_output, impact_cost = measles_intervention.impact_calculation(
            source_fname=measles_target,
            q1=self.num_f_recipients,
            q2=self.num_f_intervention_events,
            q3=self.quantity_f_reatment_r_transaction,
            q3_norm=self.normalized_dose_r_treatment,
            q3_1=self.q3_1,
            q3_2=None,
            q3_3=None,
            q3_4=None,
            interv_window=self.interv_window,
            reporting_dur=self.reporting_dur,
        )

        post_intervention1 = intervention_output.to_dataframe().reset_index()
        post_intervention1 = gpd.GeoDataFrame(post_intervention1)
        post_intervention1 = post_intervention1.dropna()
        post_intervention1["diff"] = (
            post_intervention1["baseline"] - post_intervention1["intervention"]
        )
        post_intervention1["cost"] = impact_cost
        post_intervention1["admin0"] = self.country_level
        out_df = post_intervention1.drop("geometry", 1)
        out_df = pd.DataFrame(out_df)
        with self.output().open("w") as out:
            out_df.to_csv(out.name, index=False)
