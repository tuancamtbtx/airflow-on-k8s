import os
from typing import Dict, Any
class DagCompiler:
    def __init__(self, path_config:str=None, **kwargs):
        self.path_config = path_config
    
    def compile(self, dag:Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return 
    
    def load(self, path:str) -> Dict[str, Any]:
        return
    
    
def load_yaml_dags(
    globals_dict: Dict[str, Any],
    dags_folder: str = airflow_conf.get("core", "dags_folder"),
    suffix=None,
):
    """
    Loads all the yaml/yml files in the dags folder

    The dags folder is defaulted to the airflow dags folder if unspecified.
    And the prefix is set to yaml/yml by default. However, it can be
    interesting to load only a subset by setting a different suffix.

    :param globals_dict: The globals() from the file used to generate DAGs
    :dags_folder: Path to the folder you want to get recursively scanned
    :suffix: file suffix to filter `in` what files to scan for dags
    """
    # chain all file suffixes in a single iterator
    logging.info("Loading DAGs from %s", dags_folder)
    if suffix is None:
        suffix = [".yaml", ".yml"]
    candidate_dag_files = []
    for suf in suffix:
        candidate_dag_files = chain(
            candidate_dag_files, Path(dags_folder).rglob(f"*{suf}")
        )

    for config_file_path in candidate_dag_files:
        config_file_abs_path = str(config_file_path.absolute())
        DagFactory(config_file_abs_path).generate_dags(globals_dict)
        logging.info("DAG loaded: %s", config_file_path)