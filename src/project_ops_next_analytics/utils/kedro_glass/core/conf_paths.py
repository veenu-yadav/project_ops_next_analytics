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
"""Functionality to resolve conf paths to include confs found in connectors."""

import glob
import warnings
from typing import Iterable


def get_connector_conf_paths(
    conf_paths: Iterable[str],
    project_path: str,
    connector_glob: str = "{}/src/**/connectors/**/conf/**/",
    ensure_local: bool = True,
) -> Iterable[str]:
    """Resolves the configuration paths to include configuration found in connectors.

    Args:
        conf_paths: a list of the provided conf paths
        project_path: the path to the kedro project's root
        connector_glob: a glob pattern to match connector directories
        ensure_local: if True ensures that conf/local is included in the resolved
            conf_paths. By default, kedro does
        not include conf/local in the conf paths if an env is supplied.

    Returns:
        A list of the resolved conf paths.
    """
    if ensure_local:
        conf_paths = _ensure_local_is_in_path(conf_paths)

    glass_confs = _get_connector_conf_paths(project_path, connector_glob)
    return glass_confs + conf_paths


def _ensure_local_is_in_path(conf_paths: Iterable[str]):
    if any("conf/local" in path for path in conf_paths):
        return conf_paths

    if "conf/base" not in conf_paths[0]:
        warnings.warn(
            f"""conf/base not found in first conf path ({conf_paths[0]})
            when generating local conf. This may produce unexpected results."""
        )
    conf_dir = conf_paths[0][
        :-5
    ]  # removes the last five characters (i.e. removes /base).

    return conf_paths + [f"{conf_dir}/local"]


def _get_connector_conf_paths(project_path: str, connector_glob: str):
    return [
        *glob.glob(
            pathname=connector_glob.format(str(project_path)),
            recursive=True,
        )
    ]
