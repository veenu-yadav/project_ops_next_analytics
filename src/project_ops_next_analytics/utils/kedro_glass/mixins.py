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

# pylint: skip-file

"""An Extension of the kedro context to facilitate config driven pipelines."""

import glob
import warnings
from typing import Dict, List

from kedro.config import ConfigLoader, MissingConfigException
from kedro.framework.context import KedroContextError
from kedro.pipeline import Pipeline, node, pipeline
from kedro.utils import load_obj


class GlassContextMixin:
    """An Extension of the kedro context to facilitate config driven pipelines."""

    warnings.warn(
        """The GlassContextMixin (and the mixin system) has been deprecated.
        Please move your kedro projects to 0.17+ and upgrade glass to 0.8+""",
        DeprecationWarning,
    )

    @property
    def pipeline(self) -> Dict[str, Pipeline]:
        """The kedro pipeline attribute."""
        return self._get_pipelines()

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        """Returns the pipelines dictionary."""
        return self._get_config_pipelines()

    def _get_config_pipelines(self) -> Dict[str, Pipeline]:
        """Builds a pipeline by searching for the nodes with the specified prefix.

        Returns:
            A Kedro pipeline

        Raises:
            MissingConfigException: if nodes can't be found
        """
        pipelines = self.pipelines_config
        nodes = self.nodes_config

        missing_nodes = list()
        for _pipeline in pipelines.values():
            for _node in _pipeline["nodes"]:
                if _node not in nodes:
                    missing_nodes.append(_node)

        if len(missing_nodes) > 0:
            raise MissingConfigException(
                "The following nodes cannot be found in nodes configs {}".format(  # pylint: disable=consider-using-f-string, line-too-long # noqa: E501
                    set(missing_nodes)
                )
            )

        def default_pipeline_selected():
            raise KedroContextError(
                "Please specify a pipeline to run using the --pipeline command "
                "line argument or to specify a default "
                "pipeline with the name default."
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
            default_pipeline = {
                "__default__": generate_pipeline(pipelines.get("default"))
            }
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

    @property
    def pipelines_config(self):
        """The pipeline metadata."""
        return self._get_pipelines_config()

    def _get_pipelines_config(self, pipelines_pattern: str = "**/pipelines*") -> Dict:
        """Gets the pipelines config from specified conf paths.

        Args:
            pipelines_pattern: pattern to look for pipelines

        Returns:
            A dictionary of the pipelines
        """
        raw_pipelines_config = self.config_loader.get(pipelines_pattern)
        pipelines_config = dict()
        for pipeline_name in raw_pipelines_config:
            all_sub_pipelines = self._collect_pipelines_recursively(
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

    def _collect_pipelines_recursively(
        self, pipelines: List, pipelines_config: Dict
    ) -> List:
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
                f"The following pipeline cannot be found in pipelines "
                f"configs {missing_pipeline}."
            )

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
            result = self._collect_pipelines_recursively(
                all_pipelines, pipelines_config
            )
        else:
            result = pipelines

        return result

    @property
    def nodes_config(self):
        """Returns the node config, with node name keys and kedro node values."""
        return self._get_nodes_config()

    def _get_nodes_config(self, nodes_pattern: str = "**/nodes*") -> Dict:
        """Fetches nodes from config.

        Args:
            nodes_pattern: the nodes files pattern

        Returns:
            A node dictionary

        Raises:
            TypeError: if the node can't be built
        """
        nodes_config = self.config_loader.get(nodes_pattern)

        updated_nodes = dict()
        for node_name, contents in nodes_config.items():
            assert (
                contents["func"] is not None
            ), "you forgot to define a function under func within the node {}".format(  # pylint: disable=consider-using-f-string, line-too-long # noqa: E501
                node_name
            )

            func = load_obj(contents["func"])

            updated_nodes[node_name] = {
                **{"func": func, "name": node_name},
                **{_k: _v for _k, _v in contents.items() if _k != "func"},
            }

        nodes_dict = dict()
        for name, node_config in updated_nodes.items():
            try:
                nodes_dict[name] = node(**node_config)
            except TypeError as err:
                raise TypeError(
                    "Unable to build node {}: {}".format(  # pylint: disable=consider-using-f-string, line-too-long # noqa: E501
                        name, str(err)
                    )
                )

        return nodes_dict


class GlassConnectorMixin:
    """Kedro context mixin to support discovery of configuration in connectors."""

    warnings.warn(
        """The GlassConnectorMixin (and the mixin system) has been deprecated.
        Please move your kedro projects to 0.17+ and upgrade glass to 0.8+""",
        DeprecationWarning,
    )

    CONNECTOR_GLOB = "{}/src/**/connectors/**/conf/**/"

    def _get_config_loader(self) -> ConfigLoader:
        """A hook for changing the creation of a ConfigLoader instance.

        Returns:
            Instance of `ConfigLoader` created by `_create_config_loader()`.

        """
        conf_paths = self.get_conf_paths()

        return self._create_config_loader(conf_paths)

    def _get_glass_confs(self):
        return [
            *glob.glob(
                pathname=self._get_connector_glob().format(  # pylint: disable=consider-using-f-string, line-too-long # noqa: E501
                    str(self.project_path)
                ),
                recursive=True,
            )
        ]

    def get_conf_paths(self) -> List:
        """Facilitates loading of config.

        Facilitates loading of configurations from connectors, base, custom and
        local config environments.

        The hierarchy is
            local -(overrides)-> custom -(overrides)-> base -(overrides)-> connectors.

        Returns:
            A list of conf paths to load configurations from.
        """
        conf_paths = self._get_glass_confs() + [
            str(self.project_path / self.CONF_ROOT / "base"),
            str(self.project_path / self.CONF_ROOT / str(self.env)),
            str(self.project_path / self.CONF_ROOT / "local"),
        ]
        return conf_paths

    def _get_connector_glob(self):
        return self.CONNECTOR_GLOB
