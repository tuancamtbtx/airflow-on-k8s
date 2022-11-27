import os
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.contrib.hooks.ssh_hook import SSHHook
from aircake.logger import loggerFactory

from stat import S_ISDIR, S_ISREG

logger = loggerFactory(__name__)

class SFTPGetMultipleFilesOperator(BaseOperator):

    template_fields = ('local_directory', 'remote_directory', 'remote_host')

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_directory=None,
        remote_directory=None,
        confirm=True,
        create_intermediate_dirs=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_directory = local_directory
        self.remote_directory = remote_directory
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs

    
    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    logger.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    logger.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                logger.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                all_files = _get_only_file(sftp_client, self.remote_directory )
                logger.info(f'Found {len(all_files)} files on server')
                local_folder = os.path.dirname(self.local_directory)
                if self.create_intermediate_dirs:
                    Path(local_folder).mkdir(parents=True, exist_ok=True)

                for f in all_files:
                    logger.info(f"Starting to transfer from /{self.remote_directory}/{f} to {self.local_directory}/{f}")
                    sftp_client.get(f'/{self.remote_directory}/{f}', f'/{self.local_directory}/{f}')
                    sftp_client.remove(f'/{self.remote_directory}/{f}')
        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_directory


def _get_only_file(sftp_client, remote_directory):
    results  = []
    for entry in sftp_client.listdir_attr(remote_directory):
        mode = entry.st_mode
        if S_ISDIR(mode):
            print(entry.filename + " is folder")
        elif S_ISREG(mode):
            print(entry.filename + " is file")
            results.append(f'{entry.filename}')
    return results

def _make_intermediate_dirs(sftp_client, remote_directory) -> None:
    """
    Create all the intermediate directories in a remote host

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    :return:
    """
    if remote_directory == '/':
        sftp_client.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp_client.chdir(remote_directory)
    except OSError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return