import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook as GoogleCloudBaseHook


# noinspection PyAbstractClass
class GoogleDriveHook(GoogleCloudBaseHook):
    """
    Hook for the Google Drive APIs.

    :param api_version: API version used (for example v3).
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    _conn = None

    def __init__(
        self, api_version="v3", gcp_conn_id="google_cloud_default", delegate_to=None
    ):
        super(GoogleDriveHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves the connection to Google Drive.

        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "drive", self.api_version, http=http_authorized, cache_discovery=False
            )
        return self._conn

    def _ensure_folders_exists(self, path, parent_id=None):
        service = self.get_conn()
        current_parent = parent_id or "root"
        folders = path.split("/")
        depth = 0
        # First tries to enter directories
        for current_folder in folders:
            self.log.info(
                "Looking for %s directory with %s parent",
                current_folder,
                current_parent,
            )
            conditions = [
                "mimeType = 'application/vnd.google-apps.folder'",
                "name='{}'".format(current_folder),
                "'{}' in parents".format(current_parent),
            ]
            result = (
                service.files()  # pylint: disable=no-member
                .list(
                    q=" and ".join(conditions), spaces="drive", fields="files(id, name)"
                )
                .execute(num_retries=self.num_retries)
            )
            files = result.get("files", [])
            if not files:
                self.log.info("Not found %s directory", current_folder)
                # If the directory does not exist, break loops
                break
            depth += 1
            current_parent = files[0].get("id")

        # Check if there are directories to process
        if depth != len(folders):
            # Create missing directories
            for current_folder in folders[depth:]:
                file_metadata = {
                    "name": current_folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [current_parent],
                }
                file = (
                    service.files()  # pylint: disable=no-member
                    .create(body=file_metadata, fields="id")
                    .execute(num_retries=self.num_retries)
                )
                self.log.info("Created %s directory", current_folder)

                current_parent = file.get("id")
        # Return the ID of the last directory
        return current_parent

    def upload_file(self, local_location, remote_location, parent_id=None):
        """
        Uploads a file that is available locally to a Google Drive service.

        :param local_location: The path where the file is available.
        :type local_location: str
        :param remote_location: The path where the file will be send
        :type remote_location: str
        :return: File ID
        :rtype: str
        """
        directory_path, _, filename = remote_location.rpartition("/")
        service = self.get_conn()
        if directory_path:
            parent = self._ensure_folders_exists(directory_path, parent_id)
        else:
            parent = parent_id or "root"

        file_metadata = {"name": filename, "parents": [parent]}
        self.log.info("Start to update with metadata %s", json.dumps(file_metadata))
        media = MediaFileUpload(local_location)

        # check filename exist
        file_id = self.find_by_name(filename, parent)
        if file_id:
            self.log.info(
                "[Exist] File %s Updated to gdrive://%s/%s.",
                local_location,
                parent,
                remote_location,
            )
            service.files().update(
                fileId=file_id,
                media_body=media,
            ).execute()
            return file_id

        file = (
            service.files()  # pylint: disable=no-member
            .create(body=file_metadata, media_body=media, fields="id")
            .execute(num_retries=self.num_retries)
        )
        self.log.info(
            "[Created] File %s uploaded to gdrive://%s/%s.",
            local_location,
            parent,
            remote_location,
        )
        return file.get("id")

    def find_by_name(self, name, parent_id) -> str:
        service = self.get_conn()
        conditions = [
            "name = '{}'".format(name),
            "'{}' in parents".format(parent_id),
        ]
        result = (
            service.files()  # pylint: disable=no-member
            .list(
                q=" and ".join(conditions), spaces="drive", fields="files(id, name)"
            )
            .execute(num_retries=self.num_retries)
        )
        f = result.get("files", [])
        if not f:
            return None
        return f[0].get('id')
