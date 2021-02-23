# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import os
import shutil
import tempfile
import unittest
import uuid
from contextlib import contextmanager
from typing import Generator, Optional
from unittest.mock import MagicMock, patch

from iopath.common import file_io
from iopath.common.file_io import HTTPURLHandler, LazyPath, PathManager, get_cache_dir
from iopath.common.file_io import PathManagerFactory, g_pathmgr


class TestNativeIO(unittest.TestCase):
    _tmpdir: Optional[str] = None
    _filename: Optional[str] = None
    _tmpfile: Optional[str] = None
    _tmpfile_contents = "Hello, World"
    _pathmgr = PathManager()

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmpdir = tempfile.mkdtemp()
        cls._filename = "test.txt"
        # pyre-ignore
        with open(os.path.join(cls._tmpdir, cls._filename), "w") as f:
            cls._tmpfile = f.name
            f.write(cls._tmpfile_contents)
            f.flush()

    @classmethod
    def tearDownClass(cls) -> None:
        # Cleanup temp working dir.
        if cls._tmpdir is not None:
            shutil.rmtree(cls._tmpdir)  # type: ignore

    def setUp(self) -> None:
        # Reset class variables set by methods before each test.
        self._pathmgr.set_cwd(None)
        self._pathmgr._async_handlers.clear()

    def test_open(self) -> None:
        # pyre-ignore
        with self._pathmgr.open(self._tmpfile, "r") as f:
            self.assertEqual(f.read(), self._tmpfile_contents)

    def test_factory_open(self) -> None:
        with g_pathmgr.open(self._tmpfile, "r") as f:
            self.assertEqual(f.read(), self._tmpfile_contents)

        _pathmgr = PathManagerFactory.get("test_pm")
        with _pathmgr.open(self._tmpfile, "r") as f:
            self.assertEqual(f.read(), self._tmpfile_contents)

        PathManagerFactory.remove("test_pm")

    def test_open_args(self) -> None:
        self._pathmgr.set_strict_kwargs_checking(True)
        f = self._pathmgr.open(
            self._tmpfile,  # type: ignore
            mode="r",
            buffering=1,
            encoding="UTF-8",
            errors="ignore",
            newline=None,
            closefd=True,
            opener=None,
        )
        f.close()

    def test_opena(self) -> None:
        _tmpfile = os.path.join(self._tmpdir, "async.txt")
        try:
            # Write the files.
            with self._pathmgr.opena(_tmpfile+"f", "w") as f:
                f.write("f1 ")
                with self._pathmgr.opena(_tmpfile+"g", "w") as g:
                    f.write("f2 ")
                    g.write("g1 ")
                    f.write("f3 ")
                f.write("f4 ")
            with self._pathmgr.opena(_tmpfile+"f", "a") as f:
                f.write("f5 ")
            F_STR = "f1 f2 f3 f4 f5 "
            G_STR = "g1 "

            # Test that `PathManager._async_handlers` keeps track of all
            # `PathHandler`-s where `opena` is used.
            self.assertCountEqual(
                [type(handler) for handler in self._pathmgr._async_handlers],
                [type(self._pathmgr._native_path_handler)],
            )
            # Test that 2 paths were properly logged in `NonBlockingIOManager`.
            manager = (
                self._pathmgr._native_path_handler._non_blocking_io_manager
            )
            self.assertEqual(len(manager._path_to_data), 2)
        finally:
            # Join the threads to wait for files to be written.
            self.assertTrue(self._pathmgr.async_close())

        # Check that both files were asynchronously written and written in order.
        with self._pathmgr.open(_tmpfile+"f", "r") as f:
            self.assertEqual(f.read(), F_STR)
        with self._pathmgr.open(_tmpfile+"g", "r") as g:
            self.assertEqual(g.read(), G_STR)
        # Test that both `NonBlockingIO` objects `f` and `g` are finally closed.
        self.assertEqual(len(manager._path_to_data), 0)

    def test_opena_join_behavior(self) -> None:
        _tmpfile = os.path.join(self._tmpdir, "async.txt")
        _tmpfile_contents = "Async Text"
        try:
            for _ in range(1):          # Opens 1 thread
                with self._pathmgr.opena(_tmpfile+"1", "w") as f:
                    f.write(f"{_tmpfile_contents}-1")
            for _ in range(2):          # Opens 2 threads
                with self._pathmgr.opena(_tmpfile+"2", "w") as f:
                    f.write(f"{_tmpfile_contents}-2")
            for _ in range(3):          # Opens 3 threads
                with self._pathmgr.opena(_tmpfile+"3", "w") as f:
                    f.write(f"{_tmpfile_contents}-3")
            _path_to_data = (
                self._pathmgr._native_path_handler._non_blocking_io_manager._path_to_data
            )
            # Join the threads for the 1st and 3rd file and ensure threadpool completed.
            _path_to_data_copy = dict(_path_to_data)
            self.assertTrue(
                self._pathmgr.async_join(_tmpfile+"1", _tmpfile+"3")      # Removes paths from `_path_to_io`.
            )
            self.assertFalse(_path_to_data_copy[_tmpfile+"1"].thread.is_alive())
            self.assertFalse(_path_to_data_copy[_tmpfile+"3"].thread.is_alive())
            self.assertEqual(len(_path_to_data), 1)            # 1 file remaining
        finally:
            # Join all the remaining threads
            _path_to_data_copy = dict(_path_to_data)
            self.assertTrue(self._pathmgr.async_close())

        # Ensure data cleaned up.
        self.assertFalse(_path_to_data_copy[_tmpfile+"2"].thread.is_alive())
        self.assertEqual(len(self._pathmgr._async_handlers), 0)
        self.assertEqual(len(_path_to_data), 0)                     # 0 files remaining

    def test_opena_write_buffer(self) -> None:
        _file = os.path.join(self._tmpdir, "async.txt")

        try:
            # Test IO doesn't flush until buffer is full.
            f = self._pathmgr.opena(_file, "w", buffering=10)
            with patch.object(f.buffer, 'flush', wraps=f.buffer.flush) as mock_flush:
                with patch.object(f.buffer, '_notify_manager', wraps=f.buffer._notify_manager) as mock_notify_manager:
                    f.write("."*9)
                    mock_flush.assert_not_called()      # buffer not filled - don't flush
                    mock_notify_manager.assert_not_called()
                    # Should flush when full.
                    f.write("."*13)
                    mock_flush.assert_called_once()     # buffer filled - should flush
                    # Should notify manager 3 times: 3 write calls.
                    # Buffer is split into chunks of size 10, 10, and 2.
                    self.assertEqual(mock_notify_manager.call_count, 3)
                    mock_notify_manager.reset_mock()
                    # Should notify manager 1 time: 1 close call.
                    f.close()
                    self.assertEqual(mock_notify_manager.call_count, 1)

            # Test IO flushes on file close.
            f = self._pathmgr.opena(_file, "a", buffering=10)
            with patch.object(f.buffer, 'flush', wraps=f.buffer.flush) as mock_flush:
                f.write("."*5)
                mock_flush.assert_not_called()
                f.close()
                mock_flush.assert_called()              # flush on exit
        finally:
            self.assertTrue(self._pathmgr.async_close())

        with self._pathmgr.open(_file, "r") as f:
            self.assertEqual(f.read(), "."*27)

    def test_opena_normpath(self) -> None:
        _filename = "async.txt"
        # `_file1` and `_file2` should represent the same path but have different
        # string representations.
        _file1 = os.path.join(self._tmpdir, _filename)
        _file2 = os.path.join(self._tmpdir, ".", _filename)
        self.assertNotEqual(_file1, _file2)
        try:
            _file1_text = "File1 text"
            _file2_text = "File2 text"
            with self._pathmgr.opena(_file1, "w") as f:
                f.write(_file1_text)
            with self._pathmgr.opena(_file2, "a") as f:
                f.write(_file2_text)
            _path_to_data = (
                self._pathmgr._native_path_handler._non_blocking_io_manager._path_to_data
            )
            # Check that `file2` is marked as the same file as `file1`.
            self.assertEqual(len(_path_to_data), 1)
            self.assertTrue(self._pathmgr.async_join())
            # Check that both file paths give the same file contents.
            with self._pathmgr.open(_file1, "r") as f:
                self.assertEqual(f.read(), _file1_text + _file2_text)
            with self._pathmgr.open(_file2, "r") as f:
                self.assertEqual(f.read(), _file1_text + _file2_text)
        finally:
            self.assertTrue(self._pathmgr.async_close())

    def test_opena_consecutive_join_calls(self) -> None:
        _file = os.path.join(self._tmpdir, "async.txt")
        try:
            self.assertTrue(self._pathmgr.async_join())
            try:
                with self._pathmgr.opena(_file, "w") as f:
                    f.write("1")
            finally:
                self.assertTrue(self._pathmgr.async_join())
            with self._pathmgr.open(_file, "r") as f:
                self.assertEqual(f.read(), "1")

            try:
                f = self._pathmgr.opena(_file, "a")
                f.write("2")
                f.close()
            finally:
                self.assertTrue(self._pathmgr.async_join())
            with self._pathmgr.open(_file, "r") as f:
                self.assertEqual(f.read(), "12")
        finally:
            self.assertTrue(self._pathmgr.async_close())

    def test_opena_args(self) -> None:
        _file = os.path.join(self._tmpdir, "async.txt")
        try:
            # Make sure that `opena` args are used correctly by using
            # different newline args.
            with self._pathmgr.opena(_file, "w", newline="\r\n") as f:
                f.write("1\n")
            with self._pathmgr.opena(_file, "a", newline="\n") as f:
                f.write("2\n3")
        finally:
            self.assertTrue(self._pathmgr.async_close())

        # Read the raw file data without converting newline endings to see
        # if the `opena` args were used correctly.
        with self._pathmgr.open(_file, "r", newline="") as f:
            self.assertEqual(f.read(), "1\r\n2\n3")

    def test_get_local_path(self) -> None:
        self.assertEqual(
            # pyre-ignore
            self._pathmgr.get_local_path(self._tmpfile),
            self._tmpfile,
        )

    def test_exists(self) -> None:
        # pyre-ignore
        self.assertTrue(self._pathmgr.exists(self._tmpfile))
        # pyre-ignore
        fake_path = os.path.join(self._tmpdir, uuid.uuid4().hex)
        self.assertFalse(self._pathmgr.exists(fake_path))

    def test_isfile(self) -> None:
        self.assertTrue(self._pathmgr.isfile(self._tmpfile))  # pyre-ignore
        # This is a directory, not a file, so it should fail
        self.assertFalse(self._pathmgr.isfile(self._tmpdir))  # pyre-ignore
        # This is a non-existing path, so it should fail
        fake_path = os.path.join(self._tmpdir, uuid.uuid4().hex)  # pyre-ignore
        self.assertFalse(self._pathmgr.isfile(fake_path))

    def test_isdir(self) -> None:
        # pyre-ignore
        self.assertTrue(self._pathmgr.isdir(self._tmpdir))
        # This is a file, not a directory, so it should fail
        # pyre-ignore
        self.assertFalse(self._pathmgr.isdir(self._tmpfile))
        # This is a non-existing path, so it should fail
        # pyre-ignore
        fake_path = os.path.join(self._tmpdir, uuid.uuid4().hex)
        self.assertFalse(self._pathmgr.isdir(fake_path))

    def test_ls(self) -> None:
        # Create some files in the tempdir to ls out.
        root_dir = os.path.join(self._tmpdir, "ls")  # pyre-ignore
        os.makedirs(root_dir, exist_ok=True)
        files = sorted(["foo.txt", "bar.txt", "baz.txt"])
        for f in files:
            open(os.path.join(root_dir, f), "a").close()

        children = sorted(self._pathmgr.ls(root_dir))
        self.assertListEqual(children, files)

        # Cleanup the tempdir
        shutil.rmtree(root_dir)

    def test_mkdirs(self) -> None:
        # pyre-ignore
        new_dir_path = os.path.join(self._tmpdir, "new", "tmp", "dir")
        self.assertFalse(self._pathmgr.exists(new_dir_path))
        self._pathmgr.mkdirs(new_dir_path)
        self.assertTrue(self._pathmgr.exists(new_dir_path))

    def test_copy(self) -> None:
        _tmpfile_2 = self._tmpfile + "2"  # pyre-ignore
        _tmpfile_2_contents = "something else"
        with open(_tmpfile_2, "w") as f:
            f.write(_tmpfile_2_contents)
            f.flush()
        self.assertTrue(
            self._pathmgr.copy(self._tmpfile, _tmpfile_2, overwrite=True)
        )
        with self._pathmgr.open(_tmpfile_2, "r") as f:
            self.assertEqual(f.read(), self._tmpfile_contents)

    def test_move(self) -> None:
        _tmpfile_2 = self._tmpfile + "2" + uuid.uuid4().hex # pyre-ignore
        _tmpfile_3 = self._tmpfile + "3_" + uuid.uuid4().hex # pyre-ignore
        _tmpfile_2_contents = "Hello Move"
        with open(_tmpfile_2, "w") as f:
            f.write(_tmpfile_2_contents)
            f.flush()
        # pyre-ignore
        self.assertTrue(self._pathmgr.mv(_tmpfile_2, _tmpfile_3))
        with self._pathmgr.open(_tmpfile_3, "r") as f:
            self.assertEqual(f.read(), _tmpfile_2_contents)
        self.assertFalse(self._pathmgr.exists(_tmpfile_2))
        self._pathmgr.rm(_tmpfile_3)

    def test_symlink(self) -> None:
        _symlink = self._tmpfile + "_symlink"  # pyre-ignore
        self.assertTrue(self._pathmgr.symlink(self._tmpfile, _symlink))  # pyre-ignore
        with self._pathmgr.open(_symlink) as f:
            self.assertEqual(f.read(), self._tmpfile_contents)
        self.assertEqual(os.readlink(_symlink), self._tmpfile)
        os.remove(_symlink)

    def test_rm(self) -> None:
        # pyre-ignore
        with open(os.path.join(self._tmpdir, "test_rm.txt"), "w") as f:
            rm_file = f.name
            f.write(self._tmpfile_contents)
            f.flush()
        self.assertTrue(self._pathmgr.exists(rm_file))
        self.assertTrue(self._pathmgr.isfile(rm_file))
        self._pathmgr.rm(rm_file)
        self.assertFalse(self._pathmgr.exists(rm_file))
        self.assertFalse(self._pathmgr.isfile(rm_file))

    def test_set_cwd(self) -> None:
        # File not found since cwd not set yet.
        self.assertFalse(self._pathmgr.isfile(self._filename))
        self.assertTrue(self._pathmgr.isfile(self._tmpfile))
        # Once cwd is set, relative file path works.
        self._pathmgr.set_cwd(self._tmpdir)
        self.assertTrue(self._pathmgr.isfile(self._filename))

        # Set cwd to None
        self._pathmgr.set_cwd(None)
        self.assertFalse(self._pathmgr.isfile(self._filename))
        self.assertTrue(self._pathmgr.isfile(self._tmpfile))

        # Set cwd to invalid path
        with self.assertRaises(ValueError):
            self._pathmgr.set_cwd("/nonexistent/path")

    def test_get_path_with_cwd(self) -> None:
        self._pathmgr.set_cwd(self._tmpdir)
        # Make sure _get_path_with_cwd() returns correctly.
        self.assertEqual(
            self._pathmgr._native_path_handler._get_path_with_cwd(self._filename),
            self._tmpfile
        )
        self.assertEqual(
            self._pathmgr._native_path_handler._get_path_with_cwd("/abs.txt"),
            "/abs.txt"
        )

    def test_bad_args(self) -> None:
        # TODO (T58240718): Replace with dynamic checks
        with self.assertRaises(ValueError):
            self._pathmgr.copy(
                self._tmpfile, self._tmpfile, foo="foo"  # type: ignore
            )
        with self.assertRaises(ValueError):
            self._pathmgr.exists(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.get_local_path(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.isdir(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.isfile(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.ls(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.mkdirs(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.open(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.opena(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.rm(self._tmpfile, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.set_cwd(self._tmpdir, foo="foo")  # type: ignore

        self._pathmgr.set_strict_kwargs_checking(False)

        self._pathmgr.copy(
            self._tmpfile, self._tmpfile+"2", foo="foo"  # type: ignore
        )
        self._pathmgr.exists(self._tmpfile, foo="foo")  # type: ignore
        self._pathmgr.get_local_path(self._tmpfile, foo="foo")  # type: ignore
        self._pathmgr.isdir(self._tmpfile, foo="foo")  # type: ignore
        self._pathmgr.isfile(self._tmpfile, foo="foo")  # type: ignore
        self._pathmgr.ls(self._tmpdir, foo="foo")  # type: ignore
        self._pathmgr.mkdirs(self._tmpdir, foo="foo")  # type: ignore
        f = self._pathmgr.open(self._tmpfile, foo="foo")  # type: ignore
        f.close()
        # pyre-ignore
        with open(os.path.join(self._tmpdir, "test_rm.txt"), "w") as f:
            rm_file = f.name
            f.write(self._tmpfile_contents)
            f.flush()
        self._pathmgr.rm(rm_file, foo="foo")  # type: ignore


class TestHTTPIO(unittest.TestCase):
    _remote_uri = "https://www.facebook.com"
    _filename = "facebook.html"
    _pathmgr = PathManager()

    @contextmanager
    def _patch_download(self) -> Generator[None, None, None]:
        def fake_download(url: str, dir: str, *, filename: str) -> str:
            dest = os.path.join(dir, filename)
            with open(dest, "w") as f:
                f.write("test")
            return dest

        with patch.object(
            file_io, "get_cache_dir", return_value=self._cache_dir
        ), patch.object(file_io, "download", side_effect=fake_download):
            yield

    @classmethod
    def setUpClass(cls) -> None:
        # Use a unique pid based cache directory name.
        pid = os.getpid()
        cls._cache_dir: str = os.path.join(get_cache_dir(), f"{__name__}_{pid}")
        cls._pathmgr.register_handler(HTTPURLHandler())
        if os.path.exists(cls._cache_dir):
            shutil.rmtree(cls._cache_dir)
        os.makedirs(cls._cache_dir, exist_ok=True)

    def test_get_local_path(self) -> None:
        with self._patch_download():
            local_path = self._pathmgr.get_local_path(self._remote_uri)
            self.assertTrue(os.path.exists(local_path))
            self.assertTrue(os.path.isfile(local_path))

    def test_open(self) -> None:
        with self._patch_download():
            with self._pathmgr.open(self._remote_uri, "rb") as f:
                self.assertTrue(os.path.exists(f.name))
                self.assertTrue(os.path.isfile(f.name))
                self.assertTrue(f.read() != "")

    def test_open_writes(self) -> None:
        # HTTPURLHandler does not support writing, only reading.
        with self.assertRaises(AssertionError):
            with self._pathmgr.open(self._remote_uri, "w") as f:
                f.write("foobar")  # pyre-ignore

    def test_open_new_path_manager(self) -> None:
        with self._patch_download():
            path_manager = PathManager()
            with self.assertRaises(OSError):  # no handler registered
                f = path_manager.open(self._remote_uri, "rb")

            path_manager.register_handler(HTTPURLHandler())
            with path_manager.open(self._remote_uri, "rb") as f:
                self.assertTrue(os.path.isfile(f.name))
                self.assertTrue(f.read() != "")

    def test_bad_args(self) -> None:
        with self.assertRaises(NotImplementedError):
            self._pathmgr.copy(
                self._remote_uri, self._remote_uri, foo="foo"  # type: ignore
            )
        with self.assertRaises(NotImplementedError):
            self._pathmgr.exists(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.get_local_path(
                self._remote_uri, foo="foo"  # type: ignore
            )
        with self.assertRaises(NotImplementedError):
            self._pathmgr.isdir(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.isfile(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.ls(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.mkdirs(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(ValueError):
            self._pathmgr.open(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.opena(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.rm(self._remote_uri, foo="foo")  # type: ignore
        with self.assertRaises(NotImplementedError):
            self._pathmgr.set_cwd(self._remote_uri, foo="foo")  # type: ignore

        self._pathmgr.set_strict_kwargs_checking(False)

        self._pathmgr.get_local_path(self._remote_uri, foo="foo")  # type: ignore
        f = self._pathmgr.open(self._remote_uri, foo="foo")  # type: ignore
        f.close()
        self._pathmgr.set_strict_kwargs_checking(True)


class TestLazyPath(unittest.TestCase):
    _pathmgr = PathManager()

    def test_materialize(self) -> None:
        f = MagicMock(return_value="test")
        x = LazyPath(f)
        f.assert_not_called()

        p = os.fspath(x)
        f.assert_called()
        self.assertEqual(p, "test")

        p = os.fspath(x)
        # should only be called once
        f.assert_called_once()
        self.assertEqual(p, "test")

    def test_join(self) -> None:
        f = MagicMock(return_value="test")
        x = LazyPath(f)
        p = os.path.join(x, "a.txt")
        f.assert_called_once()
        self.assertEqual(p, "test/a.txt")

    def test_getattr(self) -> None:
        x = LazyPath(lambda: "abc")
        with self.assertRaises(AttributeError):
            x.startswith("ab")
        _ = os.fspath(x)
        self.assertTrue(x.startswith("ab"))

    def test_PathManager(self) -> None:
        x = LazyPath(lambda: "./")
        output = self._pathmgr.ls(x)  # pyre-ignore
        output_gt = self._pathmgr.ls("./")
        self.assertEqual(sorted(output), sorted(output_gt))

    def test_getitem(self) -> None:
        x = LazyPath(lambda: "abc")
        with self.assertRaises(TypeError):
            x[0]
        _ = os.fspath(x)
        self.assertEqual(x[0], "a")


class TestOneDrive(unittest.TestCase):
    _url = "https://1drv.ms/u/s!Aus8VCZ_C_33gQbJsUPTIj3rQu99"

    def test_one_drive_download(self) -> None:
        from iopath.common.file_io import OneDrivePathHandler

        _direct_url = OneDrivePathHandler().create_one_drive_direct_download(self._url)
        _gt_url = (
            "https://api.onedrive.com/v1.0/shares/u!aHR0cHM6Ly8xZHJ2Lm1zL3UvcyFBd"
            + "XM4VkNaX0NfMzNnUWJKc1VQVElqM3JRdTk5/root/content"
        )
        self.assertEqual(_direct_url, _gt_url)
