import unittest
import luigi
from luigi.mock import MockTarget


class MockTarget(MockTarget):
    """
    A modified Luigi MockTarget that can be used as a backend for delegating targets.

    MockTarget contains all data in-memory.

    The main purpose is unit testing workflows without writing to disk.
    """

    def __init__(
        self,
        path=None,
        is_tmp=False,
        mirror_on_stderr=False,
        format=None,  # noqa: A002
        timeout=None,
    ):
        super().__init__(
            path, is_tmp=is_tmp, format=format, mirror_on_stderr=mirror_on_stderr
        )


class LuigiTestCase(unittest.TestCase):
    """
    - Tasks registered within a test case will get unregistered in a finalizer.
    - Instance caches are cleared before and after all runs.
    - MockTarget is set as the backend for all delelegating targets.
    - The MockFileSystem is cleared before and after all runs.
    """

    def setUp(self):
        super().setUp()
        self._stashed_reg = luigi.task_register.Register._get_reg()
        luigi.task_register.Register.clear_instance_cache()

        config = luigi.configuration.get_config()
        backend_targets = {
            "kiluigi.targets.delegating.IntermediateTarget": config.get(
                "kiluigi.targets.delegating.IntermediateTarget", "backend_class"
            ),
            "kiluigi.targets.delegating.FinalTarget": config.get(
                "kiluigi.targets.delegating.FinalTarget", "backend_class"
            ),
        }
        self._old_backend_targets = backend_targets

        for key in backend_targets.keys():
            config.set(key, "backend_class", "utils.test_utils.MockTarget")
        MockTarget.fs.clear()

    def tearDown(self):
        luigi.task_register.Register._set_reg(self._stashed_reg)

        config = luigi.configuration.get_config()
        for key, value in self._old_backend_targets.items():
            config.set(key, "backend_class", value)

        super().tearDown()
        luigi.task_register.Register.clear_instance_cache()
        MockTarget.fs.clear()
