from yaml import safe_load
from simplejson import dump
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from shutil import copyfileobj

from geopandas import GeoDataFrame
from luigi.util import requires
from luigi.format import Text, Nop
from kiluigi.targets import TaggableS3Target, FinalTarget
from kiluigi.tasks import Task

from .tasks import Predict, Aggregate

# read metadata for partial inclusion in output
with (Path(__file__).parent / "economic_model.yml").open() as yml:
    model_desc = safe_load(yml)

# json target args
json_target = {
    "root_path": "s3://darpa-output-dev/darpa/final",
    "backend_class": TaggableS3Target,
    "ACL": "public-read",
}


def serializable(obj):
    """helper for json.dump to make certain objects serializable"""

    if hasattr(obj, "__geo_interface__"):
        return obj.__geo_interface__
    if hasattr(obj, "path"):
        parts = list(urlparse(obj.path))
        parts[0] = "https"
        parts[1] = f"{parts[1]}.s3.amazonaws.com"
        return {"@id": urlunparse(parts)}
    return str(obj)


@requires(Predict, Aggregate)
class Combine(Task):
    """
    Output model predictions as a single JSON object containing [references to]
    gridded and zonal data.
    """

    def output(self):
        return FinalTarget(
            path=model_desc["id"] + ".json", task=self, format=Text, **json_target,
        )

    def run(self):

        with self.input()[0].open() as f:
            gridded = f.read()

        with self.input()[1].open() as f:
            zonal = f.read()

        # create dictionary for datasets
        dd = {"groups": {"gridded": {}, "zonal": {}}}

        # copy tiles to public storage
        dd_tiles = gridded[["tiles"]].to_dict()
        tiles = dd_tiles["data_vars"]["tiles"]["data"]
        for i, v in enumerate(tiles):
            p = Path(v).relative_to(self.input()[0].root_path)
            target = FinalTarget(path=str(p), format=Nop, **json_target)
            tiles[i] = target
            with open(v, "rb") as src, target.open("w") as dst:
                copyfileobj(src, dst)

        # add the gridded group, substituting `tiles` for `data`
        gridded = gridded.drop_vars("tiles").to_dict(data=False)
        gridded["data_vars"][self.y_var]["tiles"] = dd_tiles["data_vars"]["tiles"][
            "data"
        ]
        dd_tiles.pop("data_vars")
        dd["groups"]["gridded"].update(gridded)

        # add the zonal group
        dd["groups"]["zonal"].update(zonal[[self.y_var]].to_dict())
        location = GeoDataFrame(zonal.drop_dims("period").to_dataframe())
        dd["groups"]["zonal"]["coords"]["location"] = location

        # update/delete dimensions and coordinates shared among groups
        dd.update(dd_tiles)
        for dim_k in dd["dims"]:
            for group_k in dd["groups"]:
                dd["groups"][group_k]["dims"].pop(dim_k, None)
        for coord_k in dd["coords"]:
            for group_k in dd["groups"]:
                dd["groups"][group_k]["coords"].pop(coord_k, None)

        # enrich description
        dd["attrs"].update(
            {
                "label": "Consumption Expenditure",
                "task": self.task_family,
                "description": model_desc["description"],
                "version": model_desc["version"],
                "parameters": self.to_str_params(),
            }
        )

        dd.update(serializable(self.output()))
        with self.output().open("w") as f:
            dump(dd, f, default=serializable, ignore_nan=True)
