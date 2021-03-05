# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import concurrent.futures
import io
import logging
from dataclasses import dataclass
from queue import Queue
from threading import Thread
from typing import Callable, IO, Optional, Union


"""
This file is used for asynchronous file operations.

When `opena` is called for the first time for a specific
`PathHandler`, a `NonBlockingIOManager` is instantiated. The
manager returns a `NonBlockingIO` (or `BufferedNonBlockingIO`)
instance to the caller, and the manager maintains all of the
thread management and data management.
"""

@dataclass
class PathData:
    """
    Manage the IO job queue and polling thread for a single
    path. This is done to ensure that write calls to the same
    path are serialized so they are written in the same order
    as they were called.

    On each `f.write` call where `f` is of type `NonBlockingIO`,
    we send the job to the manager where it is enqueued to the
    Queue. The polling Thread picks up on the job, executes it,
    waits for it to finish, and then continues to poll.
    """
    queue: Queue
    thread: Thread


class NonBlockingIOManager:
    """
    All `opena` calls pass through this class so that it can
    keep track of the threads for proper cleanup at the end
    of the script. Each path that is opened with `opena` is
    assigned a single queue and polling thread that is kept
    open until it is cleaned up by `PathManager.async_join()`.
    """
    def __init__(
        self,
        buffered: Optional[bool] = False,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> None:
        """
        Args:
            buffered (bool): IO instances will be `BufferedNonBlockingIO`
                or `NonBlockingIO` based on this value. This bool is set
                manually for each `PathHandler` in `_opena`.
            executor: User can optionally attach a custom executor to
                perform async operations through `PathHandler.__init__`.
        """
        self._path_to_data = {}         # Map from path to `PathData` object
        self._buffered = buffered
        self._IO = BufferedNonBlockingIO if self._buffered else NonBlockingIO
        self._pool = executor or concurrent.futures.ThreadPoolExecutor()

    def get_non_blocking_io(
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
        Called by `PathHandler._opena` with the path and returns a
        `NonBlockingIO` instance.
        """
        if path not in self._path_to_data:
            # Initialize job queue and a polling thread
            queue = Queue()
            t = Thread(target=self._poll_jobs, args=(queue,))
            t.start()
            # Store the `PathData`
            self._path_to_data[path] = PathData(queue, t)

        # `NonBlockingIO` operates only on binary data. Ensure
        # arguments are passed correctly if using text data.
        binary = "b" in mode
        buffer = self._IO(
            path,
            mode+"b" if not binary else mode,
            notify_manager=lambda io_callable: (    # Pass async jobs to manager
                self._path_to_data[path].queue.put(io_callable)
            ),
            buffering=buffering,
            encoding=encoding if binary else None,
            errors=errors if binary else None,
            newline=newline if binary else None,
            closefd=closefd,
            opener=opener,
        )
        if binary:
            return buffer
        return io.TextIOWrapper(
            buffer,
            encoding=encoding,
            errors=errors,
            newline=newline,
            line_buffering=False,
            write_through=True,
        )

    def _poll_jobs(self, queue: Optional[Callable[[], None]]) -> None:
        """
        A single thread runs this loop. It waits for an IO callable to be
        placed in a specific path's `Queue` where the queue contains
        callable functions. It then waits for the IO job to be completed
        before looping to ensure write order.
        """
        while True:
            # `func` is a callable function (specifically a lambda function)
            # and can be any of:
            #   - func = file.write(b)
            #   - func = file.close()
            #   - func = None
            func = queue.get()                      # Blocks until item read.
            if func is None:                        # Thread join signal.
                break
            self._pool.submit(func).result()        # Wait for job to finish.

    def _join(self, path: Optional[str] = None) -> bool:
        """
        Waits for write jobs for a specific path or waits for all
        write jobs for the path handler if no path is provided.

        Args:
            path (str): Pass in a file path and will wait for the
                asynchronous jobs to be completed for that file path.
                If no path is passed in, then all threads operating
                on all file paths will be joined.
        """
        if path and path not in self._path_to_data:
            raise ValueError(
                f"{path} has no async IO associated with it. "
                f"Make sure `opena({path})` is called first."
            )
        # If a `_close` call fails, we print the error and continue
        # closing the rest of the IO objects.
        paths_to_close = [path] if path else list(self._path_to_data.keys())
        success = True
        for _path in paths_to_close:
            try:
                path_data = self._path_to_data.pop(_path)
                path_data.queue.put(None)
                path_data.thread.join()
            except Exception:
                logger = logging.getLogger(__name__)
                logger.exception(
                    f"`NonBlockingIO` thread for {_path} failed to join."
                )
                success = False
        return success

    def _close_thread_pool(self) -> bool:
        """
        Closes the ThreadPool.
        """
        try:
            self._pool.shutdown()
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception(
                "`NonBlockingIO` thread pool failed to close."
            )
            return False
        return True


# NOTE: We currently only support asynchronous writes (not reads).
class NonBlockingIO(io.IOBase):
    MAX_BUFFER_BYTES = 10 * 1024 * 1024     # 10 MiB

    def __init__(
        self,
        path: str,
        mode: str,
        notify_manager: Callable[[Callable[[], None]], None],
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Optional[Callable] = None,
    ) -> None:
        """
        Returned to the user on an `opena` call. Uses a Queue to manage the
        IO jobs that need to be run to ensure order preservation and a
        polling Thread that checks the Queue. Implementation for these are
        lifted to `NonBlockingIOManager` since `NonBlockingIO` closes upon
        leaving the context block.

        NOTE: Writes to the same path are serialized so they are written in
        the same order as they were called but writes to distinct paths can
        happen concurrently.

        Args:
            path (str): a URI that implements the `PathHandler._opena` method.
            mode (str): currently must be "w" or "wb" as we only implement an
                async writing feature.
            notify_manager (Callable): a callback function passed in from the
                `NonBlockingIOManager` so that all IO jobs can be stored in
                the manager. It takes in a single argument, namely another
                callable function.
                Example usage:
                ```
                    notify_manager(lambda: file.write(data))
                    notify_manager(lambda: file.close())
                ```
                Here, we tell `NonBlockingIOManager` to add a write callable
                to the path's Queue, and then to add a close callable to the
                path's Queue. The path's polling Thread then executes the write
                callable, waits for it to finish, and then executes the close
                callable. Using `lambda` allows us to pass callables to the
                manager.
        """
        super().__init__()
        self._path = path
        self._mode = mode
        self._notify_manager = notify_manager

        # `_file` will be closed by context manager exit or when `.close()`
        # is called explicitly.
        self._file = open(  # noqa: P201
            self._path,
            self._mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
        )

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
        Called on `f.write()`. Gives the manager the write job to call.
        """
        self._notify_manager(lambda: self._file.write(b))

    def close(self) -> None:
        """
        Called on `f.close()` or automatically by the context manager.
        We add the `close` call to the file's queue to make sure that
        the file is not closed before all of the write jobs are complete.
        """
        self._notify_manager(lambda: self._file.close())


# NOTE: This class is not being used anywhere. It is here for temporary
# placement until a new structure is decided on.
class BufferedNonBlockingIO(NonBlockingIO):
    MAX_BUFFER_BYTES = 10 * 1024 * 1024     # 10 MiB

    def __init__(
        self,
        path: str,
        mode: str,
        notify_manager: Callable[[Callable[[], None]], None],
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Optional[Callable] = None,
    ) -> None:
        """
        Buffered version of `NonBlockingIO`. All write data is stored in an
        IO buffer until the buffer is full, or `flush` or `close` is called.

        Args:
            Same as `NonBlockingIO` args.
        """
        super().__init__(
            path=path,
            mode=mode,
            notify_manager=notify_manager,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
        )
        self._buffers = [io.BytesIO()]
        self._buffer_size = buffering if buffering > 0 else self.MAX_BUFFER_BYTES

    def write(self, b: Union[bytes, bytearray]) -> None:
        """
        Called on `f.write()`. Gives the manager the write job to call.
        """
        buffer = self._buffers[-1]
        with memoryview(b) as view:
            buffer.write(view)
        if buffer.tell() < self._buffer_size: return
        self.flush()

    def close(self) -> None:
        """
        Called on `f.close()` or automatically by the context manager.
        We add the `close` call to the file's queue to make sure that
        the file is not closed before all of the write jobs are complete.
        """
        self.flush()
        # Close the last buffer created by `flush`.
        self._notify_manager(lambda: self._buffers[-1].close())
        self._notify_manager(lambda: self._file.close())

    def flush(self) -> None:
        """
        Called on `f.write()` if the buffer is filled (or overfilled). Can
        also be explicitly called by user.
        NOTE: Buffering is used in a strict manner. Any buffer that exceeds
        `self._buffer_size` will be broken into multiple write jobs where
        each has a write call with `self._buffer_size` size.
        """
        buffer = self._buffers[-1]
        if buffer.tell() == 0:
            return
        pos = 0
        total_size = buffer.seek(0, io.SEEK_END)
        view = buffer.getbuffer()
        # Chunk the buffer in case it is larger than the buffer size.
        while pos < total_size:
            item = view[pos : pos + self._buffer_size]
            # `item=item` is needed due to Python's late binding closures.
            self._notify_manager(lambda item=item: self._file.write(item))
            pos += self._buffer_size
        # Close buffer immediately after being written to file and create
        # a new buffer.
        self._notify_manager(lambda: buffer.close())
        self._buffers.append(io.BytesIO())
