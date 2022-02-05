# # Copyright (c) 2016 - present
# # QuantumBlack Visual Analytics Ltd (a McKinsey company).
# # All rights reserved.
# #
# # This software framework contains the confidential and proprietary information
# # of QuantumBlack, its affiliates, and its licensors. Your use of these
# # materials is governed by the terms of the Agreement between your organisation
# # and QuantumBlack, and any unauthorised use is forbidden. Except as otherwise
# # stated in the Agreement, this software framework is for your internal use
# # only and may only be shared outside your organisation with the prior written
# # permission of QuantumBlack.
#
# # pylint: skip-file
# # flake8: noqa
#
# """Test kedro_glass."""
#
# import shutil
# import subprocess
# from pathlib import Path
#
# import pytest
#
# PROJECT_NAME = "test_project"
#
#
# @pytest.fixture
# def kedro_new(tmp_path):
#     filepath = tmp_path / "prompt.yml"
#
#     prompt_text = f"""
# project_name: {PROJECT_NAME}
# repo_name: {PROJECT_NAME}
# python_package: {PROJECT_NAME}
# """
#
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     filepath.write_text(prompt_text)
#
#     subprocess_call(
#         f"cd {tmp_path} && kedro new --starter pandas-iris --config=prompt.yml"
#     )
#
#
# @pytest.fixture
# def remove_pipeline_registry(tmp_path):
#     subprocess_call(
#         f"rm {tmp_path}/{PROJECT_NAME}/src/{PROJECT_NAME}/pipeline_registry.py"
#     )
#
#
# @pytest.fixture
# def modify_hooks(tmp_path):
#     hooks_file_txt = f"""
# from typing import Any, Dict, Iterable, Optional
#
# from kedro.config import ConfigLoader
# from kedro.framework.hooks import hook_impl
# from kedro.io import DataCatalog
# from kedro.versioning import Journal
#
# from {PROJECT_NAME}.kedro_glass.core import (
#     get_connector_conf_paths,
#     get_glass_pipelines,
# )
# from pathlib import Path
# from kedro.pipeline import Pipeline
#
#
#
# class ProjectHooks:
#
#     @hook_impl
#     def register_pipelines(self) -> Dict[str, Pipeline]:
#         "Registers the pipelines with the KedroContext using glass."
#         return get_glass_pipelines(self.config_loader)
#
#     @hook_impl
#     def register_config_loader(
#         self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any]
#     ) -> ConfigLoader:
#         project_path = Path(__file__).parents[2]
#         conf_paths = get_connector_conf_paths(
#             conf_paths=conf_paths, project_path=project_path, ensure_local=True
#         )
#
#         self.config_loader = ConfigLoader(conf_paths)
#
#         return self.config_loader
#
#     @hook_impl
#     def register_catalog(
#         self,
#         catalog: Optional[Dict[str, Dict[str, Any]]],
#         credentials: Dict[str, Dict[str, Any]],
#         load_versions: Dict[str, str],
#         save_version: str,
#         journal: Journal,
#     ) -> DataCatalog:
#         return DataCatalog.from_config(
#             catalog, credentials, load_versions, save_version, journal
#         )
# """
#     filepath = tmp_path / PROJECT_NAME / "src" / PROJECT_NAME / "hooks.py"
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     filepath.write_text(hooks_file_txt)
#
#
# @pytest.fixture
# def create_dummy_func(tmp_path):
#     identity_func_txt = """
# def identity(x):
#     return x
#     """
#     filepath = tmp_path / PROJECT_NAME / "src" / PROJECT_NAME / "identity.py"
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     filepath.write_text(identity_func_txt)
#
#
# @pytest.fixture
# def create_nodes_yml(tmp_path):
#     nodes_yml_txt = f"""
# my_node:
#   func: {PROJECT_NAME}.identity.identity
#   inputs: example_iris_data
#   outputs: my_data
#     """
#     filepath = tmp_path / PROJECT_NAME / "conf" / "base" / "nodes.yml"
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     filepath.write_text(nodes_yml_txt)
#
#
# @pytest.fixture
# def create_pipelines_yml(tmp_path):
#     pipelines_yml_txt = """
# core_connectors:
#   nodes:
#     - my_node
#
# nested_pipeline:
#   pipelines:
#     - core_connectors
#
# default:
#   pipelines:
#     - nested_pipeline
#     """
#     filepath = tmp_path / PROJECT_NAME / "conf" / "base" / "pipelines.yml"
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     filepath.write_text(pipelines_yml_txt)
#
#
# @pytest.fixture
# def copy_glass(tmp_path):
#
#     utility_path = Path(__file__).parents[2]
#
#     shutil.copytree(
#         utility_path / "kedro_glass",
#         tmp_path / PROJECT_NAME / "src" / PROJECT_NAME / "kedro_glass",
#     )
#
#
# @pytest.mark.usefixtures(
#     "kedro_new",
#     "copy_glass",
#     "modify_hooks",
#     "create_dummy_func",
#     "create_nodes_yml",
#     "create_pipelines_yml",
#     "remove_pipeline_registry",
# )
# def test_kedro_glass_simple(tmp_path):
#     subprocess_call(f"cd {tmp_path}/{PROJECT_NAME} && kedro run")
#
#     # cannot capture with caplog
#     # we need to read in the log file that kedro produces
#     with open(str(tmp_path / PROJECT_NAME / "logs" / "info.log")) as f:
#         logs_txt = f.read()
#
#     assert "Pipeline execution completed successfully." in logs_txt, logs_txt
#     assert "Completed 1 out of 1 tasks" in logs_txt, logs_txt
#     assert "Running node: my_node" in logs_txt, logs_txt
#
#
# @pytest.mark.usefixtures(
#     "kedro_new",
#     "copy_glass",
#     "modify_hooks",
#     "create_dummy_func",
#     "create_nodes_yml",
#     "create_pipelines_yml",
#     "remove_pipeline_registry",
# )
# def test_kedro_glass_pipeline(tmp_path):
#     subprocess_call(
#         f"cd {tmp_path}/{PROJECT_NAME} && kedro run --pipeline=core_connectors"
#     )
#
#     # cannot capture with caplog
#     # we need to read in the log file that kedro produces
#     with open(str(tmp_path / PROJECT_NAME / "logs" / "info.log")) as f:
#         logs_txt = f.read()
#
#     assert "Pipeline execution completed successfully." in logs_txt, logs_txt
#     assert "Completed 1 out of 1 tasks" in logs_txt, logs_txt
#     assert "Running node: my_node" in logs_txt, logs_txt
#
#
# def subprocess_call(cmd: str) -> None:
#     """Call subprocess with error check."""
#     print("=========================================")
#     print(f"Calling: {cmd}")
#     print("=========================================")
#     subprocess.run(cmd, check=True, shell=True)
