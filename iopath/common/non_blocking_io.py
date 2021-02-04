# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import concurrent.futures
import io
import logging
from queue import Queue
from typing import Callable, IO, Optional, Union


class NonBlockingIOManager:
    """
    All `opena` calls pass through this class so that it can
    keep track of the IO objects for proper cleanup at the end
    of the script. Each path that is opened with `opena` is
    assigned a single `NonBlockingIO` object that is kept open
    until it is cleaned up by `PathManager.join()` or
    automatically when an error is thrown.
    """
    # Ensure `NonBlockingIOManager` is a singleton.
    __instance = None
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
            cls.__instance._path_to_io = {}
        return cls.__instance

    def get_io_for_path(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Optional[Callable] = None,
    ) -> Union[IO[str], IO[bytes]]:
        """
        Called by `PathHandler._opena` with the path and returns
        the single `NonBlockingIO` instance attached to the path.
        """
        if path in self._path_to_io:
            return self._path_to_io[path]
        self._path_to_io[path] = NonBlockingIO(
            path,
            mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
        )
        return self._path_to_io[path]

    def _join(self, path: Optional[str] = None) -> bool:
        """
        Cleans up the ThreadPool for each of the `NonBlockingIO`
        objects and ensures all files are closed.

        Args:
            path (str): Pass in a file path and all of the threads that
                are operating on that file path will be joined. If no
                path is passed in, then all threads operating on all file
                paths will be joined.
        """
        if path and path not in self._path_to_io:
            raise ValueError(
                f"{path} has no async IO associated with it. "
                f"Make sure `opena({path})` is called first."
            )
        # If a `_close` call fails, we print the error and continue
        # closing the rest of the IO objects.
        paths_to_close = [path] if path else list(self._path_to_io.keys())
        success = True
        for _path in paths_to_close:
            try:
                self._path_to_io.pop(_path)._close()
            except Exception:
                logger = logging.getLogger(__name__)
                logger.exception(
                    f"`NonBlockingIO` object for {_path} failed to close."
                )
                success = False
        return success


# NOTE: We currently only support asynchronous writes (not reads).
class NonBlockingIO(io.IOBase):
    def __init__(
        self,
        path: str,
        mode: str,
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Optional[Callable] = None,
    ) -> None:
        """
        Manages the async writes that are called with `f.write()` for a
        specific path. Uses a ThreadPool with a single worker and a Queue
        to manage the write jobs that need to be run to ensure order
        preservation.

        NOTE: Writes to the same path are serialized so they are written in
        the same order as they were called but writes to distinct paths can
        happen concurrently.

        Args:
            path (str): a URI that implements the `PathHandler._opena` method.
            mode (str): currently must be "w" or "wb" as we only implement an
                async writing feature.
        """
        super().__init__()
        self._path = path
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._closefd = closefd
        self._opener = opener

        self._write_queue = Queue()
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._consumer = self._pool.submit(self._write)

    @property
    def name(self) -> str:
        return self._path

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def write(self, b: Union[bytes, bytearray]) -> None:
        """
        Add the write job to the queue.
        """
        self._write_queue.put(b)

    def _write(self) -> None:
        """
        The ThreadPool runs this block of code and returns when
        notified to close.
        """
        while True:
            item = self._write_queue.get()
            if item is None:        # Signal that ThreadPool should close
                break
            with open(
                self._path,
                self._mode,
                buffering=self._buffering,
                encoding=self._encoding,
                errors=self._errors,
                newline=self._newline,
                closefd=self._closefd,
                opener=self._opener,
            ) as f:
                f.write(item)

    def close(self) -> None:
        """
        Override the ContextManager `close` function so that the
        `NonBlockingIO` object remains open for the duration of
        the script until it is explicitly closed by `_close`.
        """
        return

    def _close(self) -> None:
        """
        Cleanup function called when `join` is called.
        """
        self._write_queue.put(None)
        try:
            self._consumer.result()
        finally:
            self._pool.shutdown()
            # Close fd without errors since the errors are raised in `_join`.
            super().close()
