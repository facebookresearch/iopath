# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import datetime as dt
import logging
import os
import shutil
from pathlib import Path
from typing import IO, Any, List, Optional, Tuple, Union

from google.cloud import storage
from google.cloud.storage.blob import Blob
from iopath.common.file_io import PathHandler, file_lock, get_cache_dir


class GcsPathHandler(PathHandler):
    """
    Download URLs and cache them to disk.
    """

    GCS_PREFIXES = ["gs://", "gcs://"]
    CACHE_SUBDIR_NAME = "gcs_cache"

    def __init__(
        self,
        cache_dir: Optional[str] = None,
    ):
        """
        Args:
            cache_dir (str): Local filesystem directory to use for caching. If None,
                uses default from `file_io.get_cache_dir()`.
        """
        self.cache_dir = cache_dir

    def _get_supported_prefixes(self) -> List[str]:
        return self.GCS_PREFIXES

    def _get_client(self):
        if not hasattr(self, "client"):
            self.client = storage.Client()
        return self.client

    def _get_blob(self, uri: str) -> Blob:
        bucket, gcs_path = self._parse_uri(uri)
        client = self._get_client()
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(gcs_path)
        return blob

    def _exists(self, path: str) -> bool:
        """
        Checks if there is a resource at the given URI.
        Args:
            path (str): A URI supported by this PathHandler
        Returns:
            bool: true if the path exists
        """
        return self._get_blob(path).exists()

    def _isfile(self, path: str) -> bool:
        """
        Checks if the resource at the given URI is a file.
        Args:
            path (str): A URI supported by this PathHandler
        Returns:
            bool: true if the path is a file
        """

        # NOTE: this incurs an API call.
        return not path.endswith("/") and self._exists(path)

    def _isdir(self, path: str) -> bool:
        """
        Checks if the resource at the given URI is a directory.
        Args:
            path (str): A URI supported by this PathHandler
        Returns:
            bool: true if the path is a directory
        """

        # NOTE: this incurs an API call.
        return path.endswith("/") and self._exists(path)

    def _is_local_stale(self, blob, local_path) -> bool:
        """
        Checks that the local copy of the remote blob is still valid.

        If Remote was created after Local,

        Args:
            blob (Blob): Blob object (on the server)
            local_path (str): Path to the local copy of the blob
        Returns:
            True if Remote
        """
        remote_dt = blob.time_created
        if not remote_dt:
            return False
        remote_dt = remote_dt.astimezone()  # Compare on UTC TZ
        local_dt = dt.datetime.fromtimestamp(os.path.getmtime(local_path)).astimezone()

        # Bigger means newer
        BUFFER_N_MINUTES = 2  # Let's give it a 2 min buffer just in case
        return (remote_dt - local_dt) > dt.timedelta(minutes=BUFFER_N_MINUTES)

    def _local_cache_path(
        self,
        path: str,
    ):
        """
        Helper that returns a local cache path for a given uri.
        Args:
            path (str): A URI supported by this PathHandler.
        Returns:
            local_cache_path (str): a file path which exists on the local file system,
            in a cache directory.
        """
        _bucket, file_path = self._parse_uri(path)
        return os.path.join(
            get_cache_dir(self.cache_dir), self.CACHE_SUBDIR_NAME, file_path
        )

    def _get_local_path(
        self,
        path: str,
        **kwargs: Any,
    ) -> str:
        """
        Get a filepath which is compatible with native Python I/O such as `open`
        and `os.path`.
        If URI points to a remote resource, this function may download and cache
        the resource to local disk. In this case, the cache stays on filesystem
        (under `file_io.get_cache_dir()`) and will be used by a different run.
        Therefore this function is meant to be used with read-only resources.
        Args:
            uri (str): A URI supported by this PathHandler
        Returns:
            local_path (str): a file path which exists on the local file system
        """
        logger = logging.getLogger(__name__)
        self._check_kwargs(kwargs)

        if not self._isfile(path):
            raise ValueError(f"The object at path {path} is not a file!")

        blob = self._get_blob(path)
        local_path = self._local_cache_path(path)
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with file_lock(local_path):
            if os.path.exists(local_path):
                if not self._is_local_stale(blob, local_path):
                    logger.info(
                        "URL {} was already cached in {}".format(path, local_path)
                    )
                    return local_path
                else:
                    # delete existing, we are going to re-download it
                    logger.info(
                        "URL {} was already cached in {} but copy was stale. Redownloading...".format(
                            path, local_path
                        )
                    )
                    os.unlink(local_path)

            logger.info("Caching {} ...".format(path))
            tmp = local_path + ".tmp"
            # clean-up tmp if found, because if tmp exists, it must be a dirty
            # result of a previously process that didn't cleanup itself.
            if os.path.isfile(tmp):
                os.unlink(tmp)

            try:
                with open(tmp, "wb") as fout:
                    blob.download_to_file(fout)
                    shutil.move(tmp, local_path)
            finally:
                try:
                    os.unlink(tmp)
                except Exception:
                    pass  # Not a huge deal if it stays there
            logger.info("URL {} cached in {}".format(path, local_path))
            return local_path

    def _ls(self, path: str, **kwargs: Any) -> List[str]:
        """
        List the contents of the directory at the provided URI.
        Args:
            path (str): A URI supported by this PathHandler
        Returns:
            List[str]: list of contents in given path
        """
        self._check_kwargs(kwargs)
        bucket_name, gcs_path = self._parse_uri(path)
        client = self._get_client()
        return [
            f"{blob.name}"
            for blob in client.list_blobs(bucket_name, prefix=gcs_path, delimiter="/")
            if blob.name != gcs_path  # exclude dirname
        ]

    def _rm(self, path: str, **kwargs: Any) -> None:
        """
        Remove the file (not directory) at the provided URI.
        Args:
            path (str): A URI supported by this PathHandler
        """
        self._check_kwargs(kwargs)

        blob = self._get_blob(path)
        blob.delete()

    def _mkdirs(self, path: str, **kwargs: Any) -> None:
        """
        Recursive directory creation function. Like mkdir(), but makes all
        intermediate-level directories needed to contain the leaf directory.
        Similar to the native `os.makedirs`.
        Args:
            path (str): A URI supported by this PathHandler
        """
        self._check_kwargs(kwargs)

        assert path.endswith("/"), path

        destination_blob = self._get_blob(path)
        destination_blob.upload_from_string(
            ""
        )  # https://stackoverflow.com/questions/38416598/how-to-create-an-empty-folder-on-google-storage-with-google-api

    def _open(
        self,
        path: str,
        mode: str = "r",
        # The following three arguments are unused,
        # But are included to avoid triggering WARNING
        # messages from _check_kargs.
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        read_chunk_size: Optional[int] = None,
        **kwargs: Any,
    ) -> Union[IO[str], IO[bytes]]:
        """
        Open a stream to a URI, similar to the built-in `open`.
        Args:
            path (str): A URI supported by this PathHandler
            mode (str): Specifies the mode in which the file is opened. It defaults
                to 'r'.
            buffering (int): An optional integer used to set the buffering policy.
                Pass 0 to switch buffering off and an integer >= 1 to indicate the
                size in bytes of a fixed-size chunk buffer. When no buffering
                argument is given, the default buffering policy depends on the
                underlying I/O implementation.
        Returns:
            file: a file-like object.
        """
        self._check_kwargs(kwargs)

        if "r" in mode or "w" in mode:
            blob = self._get_blob(path)
            return blob.open(mode=mode)
        else:
            return ValueError(f"Mode '{mode}' is not valid.")

    def _parse_uri(self, uri: str) -> Tuple[str, str]:
        """
        Parses a "gs://bucket/path" URI into `bucket` and `path` strings.
        Args:
            uri (str): A gs:// URI.
        Returns:
            bucket (str): the gcs bucket.
            path (str): the path on the gcs system.
        """
        _protocol, bucket_and_path = uri.split("://", 1)
        bucket, path = bucket_and_path.split("/", 1)
        return bucket, path
