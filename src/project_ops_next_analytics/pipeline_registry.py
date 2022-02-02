"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline
from project_ops_next_analytics.pipelines.de.intermediate import intermediate_pipeline
from project_ops_next_analytics.pipelines.de.primary import primary_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    de_pipeline = Pipeline([intermediate_pipeline, primary_pipeline, ])
    # ds_pipeline = Pipeline([ds_uc1_pipeline, ds_uc2_pipeline])
    # prod_sched_pipeline = Pipeline([prod_sched_intermediate_pipeline,])

    # ops_next_pipeline = de_pipeline + prod_sched_pipeline  # + ds_pipeline

    return {
        "de": de_pipeline,
        "__default__": de_pipeline,
        #  "ds": ds_pipeline,
    }
