from copy import deepcopy
from airlake.infra import TEAM_CONF
from loguru import logger
import yaml
from deepmerge import always_merger

from airlake.factory.entity import RepoType, TeamLoader
from airlake import resolve_root


GIT_SYNC_CONTAINER = {
    "name": "git-sync",
    "image": "k8s.gcr.io/git-sync/git-sync:v3.3.4",
    "env": [],
    "args": [],
    "imagePullPolicy": "IfNotPresent",
    "securityContext": {"runAsUser": 65533},
    "volumeMounts": [
        {"mountPath": "/git", "name": "gitdir", "readOnly": False},
    ],
}
DESTINATION = resolve_root("ci", "import-dags.yaml")
INJECT_PATH = "deployment_config.production-importdags.sidecarRaw"

PYTHON_REPO_VOLUME_MOUNTS = [
    {
        "mountPath": "/airlock/dags",
        "name": "storage",
        "subPath": "dags",
        "readOnly": False,
    },
    {
        "name": "airlock-bins",
        "mountPath": "/bin/git-hook.sh",
        "subPath": "git-hook.sh",
    },
]


def main():
    config_path = TEAM_CONF
    print("Loading Config path", config_path)
    team_conf = TeamLoader(config_path=config_path).load()
    containers = []
    for conf in team_conf.repos:
        over = {}
        if "github.com" in conf.repo:
            over = {
                "name": f"git-{conf.id}",
                "args": [
                    "-ssh",
                    "-ssh-known-hosts",
                    "-repo={}".format(conf.repo),
                    "-branch={}".format(conf.branch),
                    "-dest=git",
                    "-root=/git/{}".format(conf.id),
                    "-wait=5",
                    "-max-sync-failures=-1",
                ],
                "env": [],
            }
            over["volumeMounts"] = [
                {"name": "github", "mountPath": "/etc/git-secret"}
            ]
            if conf.type == RepoType.Python:
                over["args"].append("-sync-hook-command=/bin/git-hook.sh")
                over["env"].append({"name": "LOCATION", "value": conf.id})
                over["volumeMounts"].extend(deepcopy(PYTHON_REPO_VOLUME_MOUNTS))
        base = deepcopy(GIT_SYNC_CONTAINER)
        containers.append(always_merger.merge(base, over))
    yaml.Dumper.ignore_aliases = lambda *args: True
    with open(DESTINATION, "r") as w:
        template = yaml.load(w, Loader=yaml.Loader)
        start = template
        paths = INJECT_PATH.split(".")
        for key in paths[:-1]:
            start = start[key]
        start[paths[-1]] = containers

    with open(DESTINATION, "w") as w:
        logger.info("dumped {} success", DESTINATION)
        yaml.dump(template, w, sort_keys=False)


if __name__ == "__main__":
    main()
