# Copyright (c) 2016 - present
# QuantumBlack Visual Analytics Ltd (a McKinsey company).
# All rights reserved.
#
# This software framework contains the confidential and proprietary information
# of QuantumBlack, its affiliates, and its licensors. Your use of these
# materials is governed by the terms of the Agreement between your organisation
# and QuantumBlack, and any unauthorised use is forbidden. Except as otherwise
# stated in the Agreement, this software framework is for your internal use
# only and may only be shared outside your organisation with the prior written
# permission of QuantumBlack.
# pylint: disable=consider-using-f-string
"""Pipeline file.

Functionality to create kedro pipelines from pipelines defined in glass
configurations.
"""

from typing import Dict, List, Union

from kedro.config import ConfigLoader, MissingConfigException, TemplatedConfigLoader
from kedro.framework.context import KedroContextError
from kedro.pipeline import Pipeline, node, pipeline
from kedro.utils import load_obj


def get_glass_pipelines(
    config_loader: Union[ConfigLoader, TemplatedConfigLoader]
) -> Dict[str, Pipeline]:
    """Creates kedro pipelines from pipelines defined in glass configurations.

    Args:
        config_loader: The kedro project's instantiated config loader.

    Returns:
        A dictionary of kedro pipelines.
    """
    pipelines_config = _get_pipelines_config(config_loader)
    nodes_config = _get_nodes_config(config_loader)
    return _get_config_pipelines(pipelines_config, nodes_config)


def _get_config_pipelines(pipelines, nodes) -> Dict[str, Pipeline]:
    """Builds a pipeline by searching for the nodes with the specified prefix.

    Returns:
        A Kedro pipeline

    Raises:
        MissingConfigException: if nodes can't be found
    """
    missing_nodes = []
    for _pipeline in pipelines.values():
        for _node in _pipeline["nodes"]:
            if _node not in nodes:
                missing_nodes.append(_node)

    if len(missing_nodes) > 0:
        raise MissingConfigException(
            "The following nodes cannot be found in nodes configs {}".format(
                set(missing_nodes)
            )
        )

    def default_pipeline_selected():
        raise KedroContextError(
            "Please specify a pipeline to run using the --pipeline command line"
            "argument or to specify a default "
            "pipeline with the name default"
        )

    def generate_pipeline(contents):
        return pipeline(
            Pipeline(
                [nodes[node_name] for node_name in contents.get("nodes", [])],
                tags=contents["tags"],
            ),
            inputs=contents["inputs"],
            outputs=contents["outputs"],
            parameters=contents["parameters"],
            namespace=contents["namespace"],
        )

    default_pipeline_contents = pipelines.get("default")
    if default_pipeline_contents is not None:
        default_pipeline = {"__default__": generate_pipeline(pipelines.get("default"))}
    else:
        default_pipeline = {
            "__default__": Pipeline(
                [node(default_pipeline_selected, inputs=None, outputs="no_output")]
            )
        }

    return {
        **default_pipeline,
        **{
            pipeline_name: generate_pipeline(contents)
            for pipeline_name, contents in pipelines.items()
            if pipeline_name != "default"
        },
    }


def _get_pipelines_config(
    config_loader: Union[ConfigLoader, TemplatedConfigLoader],
    pipelines_pattern: str = "**/pipelines*",
) -> Dict:
    """Gets the pipelines config from specified conf paths.

    Args:
        config_loader: The kedro project's instantiated config loader.
        pipelines_pattern: pattern to look for pipelines

    Returns:
        A dictionary of the pipelines
    """
    raw_pipelines_config = config_loader.get(pipelines_pattern)
    pipelines_config = {}
    for pipeline_name in raw_pipelines_config:
        all_sub_pipelines = _collect_pipelines_recursively(
            [pipeline_name], raw_pipelines_config
        )
        pipelines_config[pipeline_name] = {
            "nodes": {
                node
                for pipeline in all_sub_pipelines
                for node in raw_pipelines_config[pipeline].get("nodes", [])
            },
            "inputs": raw_pipelines_config[pipeline_name].get("inputs"),
            "outputs": raw_pipelines_config[pipeline_name].get("outputs"),
            "parameters": raw_pipelines_config[pipeline_name].get("parameters"),
            "namespace": raw_pipelines_config[pipeline_name].get("namespace"),
            "tags": raw_pipelines_config[pipeline_name].get("tags"),
        }

    return pipelines_config


def _collect_pipelines_recursively(pipelines: List, pipelines_config: Dict) -> List:
    """Collects all of the pipelines that are called by a pipeline recursively.

    Args:
        pipelines: A list of pipelines
        pipelines_config: the pipelines config

    Returns:
        A list of all of the pipelines that are called by the
        original pipelines requested

    Raises:
        MissingConfigException: if pipelines can't be found
    """
    try:
        nested_pipelines = [pipelines_config[p].get("pipelines") for p in pipelines]
    except KeyError as missing_pipeline:
        raise MissingConfigException(
            f"The following pipeline cannot be found in pipelines"
            f"configs {missing_pipeline}"
        ) from missing_pipeline

    non_none_nested = [
        pipeline for pipeline in nested_pipelines if pipeline is not None
    ]

    if non_none_nested == [None]:
        return pipelines

    unnested_pipelines = [
        pipeline for pipeline_list in non_none_nested for pipeline in pipeline_list
    ]
    new_pipelines = set(pipelines + unnested_pipelines).difference(set(pipelines))

    if new_pipelines != set():
        all_pipelines = list(new_pipelines) + pipelines
        result = _collect_pipelines_recursively(all_pipelines, pipelines_config)
    else:
        result = pipelines

    return result


def _get_nodes_config(
    config_loader: Union[ConfigLoader, TemplatedConfigLoader],
    nodes_pattern: str = "**/nodes*",
) -> Dict:
    """Fetches nodes from config.

    Args:
        config_loader: The kedro project's instantiated config loader.
        nodes_pattern: the nodes files pattern
        config_loader: the config loader
    Returns:
        A node dictionary

    Raises:
        TypeError: if the node can't be built
    """
    nodes_config = config_loader.get(nodes_pattern)

    updated_nodes = {}
    for node_name, contents in nodes_config.items():
        assert (
            contents["func"] is not None
        ), "you forgot to define a function under func within the node {}".format(
            node_name
        )

        func = load_obj(contents["func"])

        updated_nodes[node_name] = {
            **{"func": func, "name": node_name},
            **{_k: _v for _k, _v in contents.items() if _k != "func"},
        }

    nodes_dict = {}
    for name, node_config in updated_nodes.items():
        try:
            nodes_dict[name] = node(**node_config)
        except TypeError as err:
            raise TypeError(
                "Unable to build node {}: {}".format(  # pylint: disable=consider-using-f-string  # noqa: E501
                    name, str(err)
                )
            ) from err

    return nodes_dict
