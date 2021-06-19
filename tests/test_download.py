# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import os
import unittest
import uuid
from unittest.mock import patch

from iopath.common.download import download


class TestDownload(unittest.TestCase):
    _filename = "facebook_" + uuid.uuid4().hex + ".html"

    def run(self, result=None):
        with patch("iopath.common.event_logger.EventLogger.log_event"):
            super(TestDownload, self).run(result)

    def test_download(self) -> None:
        download(
            "https://www.facebook.com", ".", filename=self._filename, progress=False
        )
        self.assertTrue(os.path.isfile(self._filename))

    def tearDown(self) -> None:
        if os.path.isfile(self._filename):
            os.unlink(self._filename)
