from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal

from project_ops_next_analytics.utils.kedro_glass.core import (
    get_connector_conf_paths,
    get_glass_pipelines,
)
from pathlib import Path
from kedro.pipeline import Pipeline
from kedro.config import TemplatedConfigLoader
import os


class ProjectHooks:

    @hook_impl
    def register_pipelines(self) -> Dict[str, Pipeline]:
        "Registers the pipelines with the KedroContext using glass."
        return get_glass_pipelines(self.config_loader)

    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any]
    ) -> TemplatedConfigLoader:
        project_path = Path(__file__).parents[2]
        conf_paths = get_connector_conf_paths(
            conf_paths=conf_paths, project_path=project_path, ensure_local=True
        )

        globals_dict = dict()

        # If the user has set parameters via the --params argument
        global_variables = ["GEN_ROOT_PREFIX", "MASTER_TABLE_NAME"]

        self.config_loader = ConfigLoader(conf_paths)

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
