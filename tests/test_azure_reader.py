# Copyright (c) Meta Platforms, Inc. and affiliates. All Rights Reserved.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import io
import unittest
from unittest.mock import Mock

from iopath.common.azure_blob import AzureBlobReader


class TestAzureReader(unittest.TestCase):
    def reader(self):
        client = Mock()
        client.download_blob.return_value.chunks.return_value = iter([b"abc", b"defg"])
        return AzureBlobReader(client, chunk_size=3)

    def test_readinto_updates_buffer_and_returns_byte_count(self):
        reader = self.reader()
        buffer = bytearray(b"xxxxx")
        self.assertEqual(reader.readinto(buffer), 3)
        self.assertEqual(buffer, b"abcxx")
        self.assertEqual(reader.readinto(memoryview(buffer)[1:]), 4)
        self.assertEqual(buffer, b"adefg")
        self.assertEqual(reader.readinto(buffer), 0)

    def test_buffered_reader_reads_across_chunks(self):
        reader = io.BufferedReader(self.reader(), buffer_size=4)
        self.assertEqual(reader.read(5), b"abcde")
        self.assertEqual(reader.read(), b"fg")

    def test_empty_buffer_does_not_consume_data(self):
        reader = self.reader()
        self.assertEqual(reader.readinto(bytearray()), 0)
        self.assertEqual(reader.read(), b"abcdefg")

    def test_readall_after_partial_read(self):
        reader = self.reader()
        self.assertEqual(reader.read(2), b"ab")
        self.assertEqual(reader.readall(), b"cdefg")
        self.assertEqual(reader.readall(), b"")

    def test_readonly_buffer_does_not_consume_data(self):
        reader = self.reader()
        with self.assertRaises(TypeError):
            reader.readinto(b"xxxx")
        self.assertEqual(reader.read(), b"abcdefg")
