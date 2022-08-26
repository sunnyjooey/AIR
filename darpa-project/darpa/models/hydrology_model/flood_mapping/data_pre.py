import os
from pathlib import Path
from subprocess import PIPE, Popen  # noqa: S404

from luigi import ExternalTask, Task
from luigi.util import requires

from kiluigi.targets import CkanTarget, IntermediateTarget

from .utils import get_data, get_mean_std, scale_data

RELATIVEPATH = "models/hydrology_model/flood_mapping/data_pre"


class DownloadSentinel1Images(ExternalTask):
    def output(self):
        dst = Path(RELATIVEPATH) / "S1Hand"
        return IntermediateTarget(path=str(dst), task=self, timeout=31536000)

    def run(self):
        with self.output().temporary_path() as tmp:
            os.makedirs(tmp, exist_ok=True)
            proc = Popen(  # noqa: S603, S607 - ignore security check
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-r",
                    "gs://sen1floods11/v1.1/data/flood_events/HandLabeled/S1Hand",
                    tmp,
                ],
                stdout=PIPE,
                stderr=PIPE,
            )
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError("Downloading S1Hand failed")


class DownloadFloodMask(ExternalTask):
    def output(self):
        dst = Path(RELATIVEPATH) / "LabelHand"
        return IntermediateTarget(path=str(dst), task=self, timeout=31536000)

    def run(self):
        with self.output().temporary_path() as tmp:
            os.makedirs(tmp, exist_ok=True)
            proc = Popen(  # noqa: S603, S607 - ignore security check
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-r",
                    "gs://sen1floods11/v1.1/data/flood_events/HandLabeled/LabelHand",
                    tmp,
                ],
                stdout=PIPE,
                stderr=PIPE,
            )
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError("Downloading LabelHand failed")


class PullDataSplit(ExternalTask):
    def output(self):
        return {
            "train": CkanTarget(
                dataset={"id": "387cf613-5895-4062-bd77-cfca116fc8bd"},
                resource={"id": "753ce395-bdad-49da-a9b3-8539a7799a42"},
            ),
            "valid": CkanTarget(
                dataset={"id": "387cf613-5895-4062-bd77-cfca116fc8bd"},
                resource={"id": "925eb7e0-09ab-41da-a9e3-c3874b266303"},
            ),
            "test": CkanTarget(
                dataset={"id": "387cf613-5895-4062-bd77-cfca116fc8bd"},
                resource={"id": "f222c692-40a2-4db9-8127-ab7d2825fb47"},
            ),
        }


@requires(DownloadSentinel1Images, DownloadFloodMask, PullDataSplit)
class TrainingData(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        img_dir = self.input()[0].path
        mask_dir = self.input()[1].path
        train_file_src = self.input()[2]["train"].path
        valid_file_src = self.input()[2]["valid"].path
        test_file_src = self.input()[2]["test"].path

        X_train, y_train = get_data(train_file_src, img_dir, mask_dir)
        X_valid, y_valid = get_data(valid_file_src, img_dir, mask_dir)
        X_test, y_test = get_data(test_file_src, img_dir, mask_dir)

        band_1_mean, band_2_mean, band_1_std, band_2_std = get_mean_std(X_train)
        X_train = scale_data(X_train, band_1_mean, band_2_mean, band_1_std, band_2_std)
        X_valid = scale_data(X_valid, band_1_mean, band_2_mean, band_1_std, band_2_std)
        X_test = scale_data(X_test, band_1_mean, band_2_mean, band_1_std, band_2_std)

        with self.output().open("w") as out:
            out.write(
                {
                    "train": (X_train, y_train),
                    "valid": (X_valid, y_valid),
                    "test": (X_test, y_test),
                }
            )
