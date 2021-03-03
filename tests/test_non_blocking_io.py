# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import os
import shutil
import tempfile
import unittest
from typing import Optional
from unittest.mock import patch

from iopath.common.non_blocking_io import (
    BufferedNonBlockingIO,
    NonBlockingIO,
    NonBlockingIOManager,
)


class TestNonBlockingIO(unittest.TestCase):
    _tmpdir: Optional[str] = None
    _io_manager = NonBlockingIOManager(buffered=False)
    _buffered_io_manager = NonBlockingIOManager(buffered=True)

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmpdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls) -> None:
        # Cleanup temp working dir.
        if cls._tmpdir is not None:
            shutil.rmtree(cls._tmpdir)  # type: ignore

    def test_select_io(self) -> None:
        self.assertEqual(self._io_manager._IO, NonBlockingIO)
        self.assertEqual(self._buffered_io_manager._IO, BufferedNonBlockingIO)

    def test_io_manager(self) -> None:
        _file = os.path.join(self._tmpdir, "non_buffered.txt")

        try:
            # Test IO notifies manager after every write call.
            f = self._io_manager.get_non_blocking_io(
                path=_file, mode="w"
            )
            with patch.object(
                f.buffer, '_notify_manager', wraps=f.buffer._notify_manager
            ) as mock_notify_manager:
                f.write("."*1)
                f.write("."*2)
                f.write("."*3)
                # Should notify manager 3 times: 3 write calls.
                self.assertEqual(mock_notify_manager.call_count, 3)
                mock_notify_manager.reset_mock()
                # Should notify manager 1 time: 1 close call.
                f.close()
                self.assertEqual(mock_notify_manager.call_count, 1)
        finally:
            self.assertTrue(self._io_manager._join())
            self.assertTrue(self._io_manager._close_thread_pool())

        with open(_file, "r") as f:
            self.assertEqual(f.read(), "."*6)

    def test_buffered_io_manager(self) -> None:
        _file = os.path.join(self._tmpdir, "buffered.txt")

        try:
            # Test IO doesn't flush until buffer is full.
            f = self._buffered_io_manager.get_non_blocking_io(
                path=_file, mode="w", buffering=10
            )
            with patch.object(f.buffer, 'flush', wraps=f.buffer.flush) as mock_flush:
                with patch.object(
                    f.buffer, '_notify_manager', wraps=f.buffer._notify_manager
                ) as mock_notify_manager:
                    f.write("."*9)
                    mock_flush.assert_not_called()      # buffer not filled - don't flush
                    mock_notify_manager.assert_not_called()
                    # Should flush when full.
                    f.write("."*13)
                    mock_flush.assert_called_once()     # buffer filled - should flush
                    # `flush` should notify manager 4 times: 3 `file.write` and 1 `buffer.close`.
                    # Buffer is split into 3 chunks of size 10, 10, and 2.
                    self.assertEqual(len(f.buffer._buffers), 2)  # 22-byte and 0-byte buffers
                    self.assertEqual(mock_notify_manager.call_count, 4)
                    mock_notify_manager.reset_mock()
                    # `close` should notify manager 2 times: 1 `buffer.close` and 1 `file.close`.
                    f.close()
                    self.assertEqual(mock_notify_manager.call_count, 2)

            # Test IO flushes on file close.
            f = self._buffered_io_manager.get_non_blocking_io(
                path=_file, mode="a", buffering=10
            )
            with patch.object(f.buffer, 'flush', wraps=f.buffer.flush) as mock_flush:
                f.write("."*5)
                mock_flush.assert_not_called()
                f.close()
                mock_flush.assert_called()              # flush on exit
        finally:
            self.assertTrue(self._buffered_io_manager._join())
            self.assertTrue(self._buffered_io_manager._close_thread_pool())

        with open(_file, "r") as f:
            self.assertEqual(f.read(), "."*27)
