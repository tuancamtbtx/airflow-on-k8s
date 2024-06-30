import time
import logging
import argparse
import os

from airflow.configuration import conf


logging.getLogger("airflow").setLevel(logging.WARN)
logging.getLogger("airflow").setLevel(logging.WARN)
logging.getLogger("schedule").setLevel(logging.WARN)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Path to local git dir")
    parser.add_argument("--git_layout", default="git/dags_conf")
    parser.add_argument(
        "--sync_roles",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--num_process", default=8, type=int, help="NumProcess that parse file!"
    )
    args = parser.parse_args()
if __name__ == "__main__":
    main()
