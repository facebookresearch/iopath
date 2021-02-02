# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import unittest

from iopath.common.non_blocking_io import NonBlockingIOManager


class TestNonBlockingIOManager(unittest.TestCase):
    def test_singleton(self) -> None:
        file_name = "random.txt"
        try:
            # Create and modify first instance.
            obj1 = NonBlockingIOManager()
            IO = obj1.get_io_for_path(file_name)
            self.assertEqual(obj1._path_to_io, {file_name: IO})
            # Try to instantiate 2nd instance.
            obj2 = NonBlockingIOManager()
            self.assertEqual(obj1, obj2)
        finally:
            obj1._join()
