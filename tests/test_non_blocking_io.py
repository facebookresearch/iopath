# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import os
import tempfile
import unittest

from iopath.common.non_blocking_io import NonBlockingIOManager


class TestNonBlockingIOManager(unittest.TestCase):
    def test_singleton(self) -> None:
        with tempfile.TemporaryDirectory() as _tmpdir:
            URI = os.path.join(_tmpdir, "test.txt")
            with open(URI, "w") as f:
                f.write("")
            try:
                # Create and modify first instance.
                obj1 = NonBlockingIOManager()
                obj1.get_non_blocking_io(URI)
                self.assertIn(URI, obj1._path_to_data)
                # Try to instantiate 2nd instance.
                obj2 = NonBlockingIOManager()
                self.assertEqual(obj1, obj2)
                self.assertIn(URI, obj2._path_to_data)
            finally:
                self.assertTrue(obj1._join())
                self.assertTrue(obj1._close_thread_pool())
