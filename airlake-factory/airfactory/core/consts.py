FIELD_DAGS_DEFAULT_ARGS = "default_args"
FIELD_DAGS_NAME = "name"
FIELD_DAGS_CONF_PATH = "conf_path"
FIELD_DAGS_ALERT = "alert"
FIELD_DAGS_ROLE_ID = "role_id"
FIELD_DAGS_SCHEDULE_INTERVAL = "schedule_interval"


class ConfigField:
    Operator = "operator"
    Subdag = "subdag"


class DefaultARGsFields:
    ParentFolder = "parent_folder"
    FileName = "file_name"
    StartDate = "start_date"
    Conns = "conns"
    RoleId = "role_id"
    Pool = "pool"


class DagFields:
    DefaultArgs = FIELD_DAGS_DEFAULT_ARGS
    Owner = "owner"
    DefaultArg = FIELD_DAGS_DEFAULT_ARGS
    Tasks = "tasks"
    Tags = "tags"
    Name = FIELD_DAGS_NAME
    Kind = "kind"
    Refs = "refs"
    SharedTasks = "shared_tasks"


class DagFieldsV2:
    Dags = "dags"


class TaskFields:
    Id = "task_id"
    Operator = "operator"
    Dependencies = "dependencies"
    HighCPUs = "high_cpus"
    Tasks = "tasks"
    ExecutorConfig = "executor_config"


class ExecutorConfig:
    PodOverrideBuilder = "pod_override_builder"
    PodOverride = "pod_override"


class OperatorName:
    K8S = "KubernetesPodOperator"
    TaskGroup = "TaskGroup"
    SubDag = "SubDagOperator"
    TimeDeltaSensor = "TimeDeltaSensor"


class SyncRoleField:
    DagName = "dag_name"
    Emails = "emails"


class SyncTeamRoleField:
    DagName = "dag_name"
    Team = "team"
    Emails = "emails"
