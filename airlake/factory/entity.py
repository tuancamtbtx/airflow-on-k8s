from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
import dacite

class RepoType:
    Python = "python"
    Yaml = "yaml"

@dataclass
class TeamConnection:
    conn_id: str
    replace_fields: List[str]
    @staticmethod
    def from_list(items: List[Dict[str, Any]]):
        return [dacite.from_dict(data_class=TeamConnection, data=i) for i in items]
    
@dataclass
class Alerting:
    kind: str
    conn_id: str

    @staticmethod
    def from_dict(conf):
        return dacite.from_dict(data_class=Alerting, data=conf)


@dataclass
class TeamConfig:
    name: str
    prefix: str
    owner: str
    repo_id: Optional[str]
    role_id: Optional[int]
    alert: Optional[Alerting]
    conns: Optional[List[TeamConnection]]
    team_dir: Optional[str]
    pool: Optional[str]
    type: Optional[str] = "yaml"

    def is_yaml(self):
        return not self.type or self.type == RepoType.Yaml

    def is_python(self):
        return self.type == RepoType.Python
