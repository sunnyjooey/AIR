import luigi

# Make sure that Luigi uses a Pythonic namespace for all Tasks
luigi.auto_namespace(scope=__name__)
