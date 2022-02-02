# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

from typing import Any

from iopath.common.file_io import PathHandler, TabularIO


class TabularUriParser:
    def parse_uri(self, uri: str) -> None:
        pass


class TabularPathHandler(PathHandler):
    def _opent(
        self, path: str, mode: str = "r", buffering: int = 32, **kwargs: Any
    ) -> TabularIO:
        assert mode == "r"
