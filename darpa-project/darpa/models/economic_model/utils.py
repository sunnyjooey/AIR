from sklearn import clone
from sklearn.model_selection import ParameterGrid


def grid(pipeline, X, y, parameters):
    """Generate a fitted clone of the pipeline for each parameter.

    Parameters
    ----------
    pipeline : estimator-like
        Scikit-learn estimator, or similar, with a `fit` method
    X : array-like
        Training data
    y : array-like
        Training targets
    parameters : dict
        Parameters to explore, with keys matching pipeline parameters and
        values giving the search scope for that parameter

    Yields
    -------
    pipeline: estimator-like

    """
    for p in ParameterGrid(parameters):
        pipeline = clone(pipeline)
        pipeline.set_params(**p)
        pipeline.fit(X, y)
        yield pipeline


def period_fill(level, rule):
    def function(data):
        return data.droplevel(level).resample(rule).pad().interpolate().bfill()

    return function
