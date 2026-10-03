# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.


import os
import tempfile
import unittest
import uuid
from unittest.mock import patch

from iopath.common.download import download


class TestDownload(unittest.TestCase):
    _filename = "facebook_" + uuid.uuid4().hex + ".html"

    # pyre-fixme[3]: Return type must be annotated.
    # pyre-fixme[2]: Parameter must be annotated.
    def run(self, result=None):
        with patch("iopath.common.event_logger.EventLogger.log_event"):
            super().run(result)

    def test_download(self) -> None:
        download(
            "https://www.facebook.com", ".", filename=self._filename, progress=False
        )
        self.assertTrue(os.path.isfile(self._filename))

    def test_url_filename_ignores_query_and_fragment(self) -> None:
        urls = [
            "https://example.com/model.bin?download=1",
            "https://example.com/model.bin?redirect=/other/name",
            "https://example.com/model.bin#section/other",
        ]
        for url in urls:
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:

                def retrieve(url, filename):
                    with open(filename, "wb") as stream:
                        stream.write(b"model data")
                    return filename, None

                with patch(
                    "iopath.common.download.request.urlretrieve", side_effect=retrieve
                ):
                    path = download(url, directory, progress=False)
                self.assertEqual(path, os.path.join(directory, "model.bin"))
                with open(path, "rb") as stream:
                    self.assertEqual(stream.read(), b"model data")

    def tearDown(self) -> None:
        if os.path.isfile(self._filename):
            os.unlink(self._filename)
