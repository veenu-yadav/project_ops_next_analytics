"""Project hooks."""
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal

import os
from typing import Any, Dict, Iterable, Optional

from kedro.config import TemplatedConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal


class ProjectHooks:
    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str]
    ) -> TemplatedConfigLoader:
        """Function to make the globals file available to kedro and making the variables available
        as params to kedro pipeline
        """
        globals_dict = dict()

        # If the user has set parameters via the --params argument
        global_variables = ["GEN_ROOT_PREFIX", "MASTER_TABLE_NAME"]

        # Convert variable name to lower case and add to globals_dict for NestedConfigLoader
        for i in global_variables:
            if os.environ.get(i, None):  # pragma: no cover
                globals_dict.update(**{i.lower(): os.environ.get(i, None)})

        return TemplatedConfigLoader(
            conf_paths,
            globals_pattern="*globals.yml",  # read the globals dictionary from project config
            globals_dict=globals_dict,  # extra keys to add to the globals dictionary,
            # take precedence over globals_pattern
        )

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )






# class ProjectHooks:
#     @hook_impl
#     def register_config_loader(
#         self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any],
#     ) -> ConfigLoader:
#         conf_paths = ["conf/base", "conf/local"]
#         return ConfigLoader(conf_paths)
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



