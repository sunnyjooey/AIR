economic_model_pipeline package
===================================

.. This is a comment that is ignored. The equal signs must be longer than the line they are underlining.

Here we add documentation concerning the overall pipeline.

The syntax is identical to that in the docstrings. For example, we can add diagrams in the same way:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :caption: This is a caption for the diagram. Hover over nodes for more details.

For more detailed information, hover your mouse over the nodes. Those that have docstrings defined will display
them as a tooltip. The diagram background tooltip lists the tasks the diagram was generated for and the date and time
it was generated. We can also specify an alternative base URL for the node hyperlinks, to jump to a task's
documentation on a different page.

We can omit tasks with the ``omit_tasks`` flag:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :omit_tasks:
    :caption: Diagram of the task and target DAG omitting tasks.

There is also an ``omit_outputs`` flag:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :omit_outputs:
    :caption: Diagram of the task and target DAG omitting output nodes.

We can filter by tag, using the ``tags`` flag - here only tasks or targets with tags ``another`` or ``this``:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :tags: another this
    :caption: Diagram including all nodes with the tags ``another`` or ``this``.

We can also filter by base class - here we only display subclasses of :class:`Task` and :class:`CkanTarget`:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :bases: kiluigi.tasks.Task kiluigi.targets.CkanTarget
    :caption: Diagram including only subclasses of :class:`kiluigi.tasks.Task` or :class:`CkanTarget`.

We can confirm these using an inheritance diagram, for example for :class:`RegridRasterForTrainingData`, :class:`ClusterDataCkanExternal` and :class:`CkanTarget`:

.. inheritance-diagram:: models.darpa_economic_model.economic_model_pipeline.RegridRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.ClusterDataCkanExternal kiluigi.targets.CkanTarget
    :caption: An inheritance diagram for :class:`RegridRasterForTrainingData`, :class:`ClusterDataCkanExternal` and :class:`CkanTarget`

We can filter by tags *and* base classes, eg, only subclasses of :class:`Target` or :class:`ExternalTask`, that also have tags ``another`` or ``this``:

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :bases: kiluigi.tasks.ExternalTask kiluigi.targets.Target
    :tags: another this
    :caption: A diagram combining base class and tag filters.

These also combine with the ``omit_~`` flags, here ``omit_outputs`` (rather pointless in this case):

.. pipeline-diagram:: models.darpa_economic_model.economic_model_pipeline.CutRasterForTrainingData models.darpa_economic_model.economic_model_pipeline.CreateSurfacePlot models.darpa_economic_model.economic_model_pipeline.UploadToCkan
    :bases: kiluigi.tasks.ExternalTask kiluigi.targets.Target
    :tags: another this
    :omit_outputs:
    :caption: A diagram combining base class, tag and omit_outputs filters.

We can tell Sphinx to extract the documentation for every task in the module:

.. automodule:: models.darpa_economic_model.economic_model_pipeline
    :members:
