import os
import yaml
import pathlib

from typing import  Dict, Any, Optional

from airfactory.common.utils import NoDatesSafeLoader
from airfactory.common.logger import LoggerMixing
class YamlReader(LoggerMixing):
    def __init__(self):
        self._check_sums = {}

    def read(self, path: os.PathLike) -> Optional[Dict[str, Any]]:
        if isinstance(path, str):
            path = pathlib.Path(path)
        try:
            content = self.read_content(path)
            if not content:
                return None
            return content
        except Exception as e:
            err = f"Error reading {path}: {e}"
            self.logger.error(err)
            return None

    @classmethod
    def read_content(self, path: os.PathLike):
        with open(path, "r") as f:
            conf = yaml.load(f, Loader=NoDatesSafeLoader)
        from airfactory.core.consts import DagFields, DefaultARGsFields, FIELD_DAGS_CONF_PATH
        default_args = conf.get(DagFields.DefaultArg) or {}

        default_args[FIELD_DAGS_CONF_PATH] = str(path)
        default_args[DefaultARGsFields.ParentFolder] = os.path.dirname(path)
        default_args[DefaultARGsFields.FileName] = os.path.basename(path)

        conf[DagFields.DefaultArg] = default_args
        return conf