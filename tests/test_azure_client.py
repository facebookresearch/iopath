# Copyright (c) Meta Platforms, Inc. and affiliates. All Rights Reserved.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import Mock, patch

from iopath.common.azure_blob import AzureBlobPathHandler


class TestAzureClient(unittest.TestCase):
    def test_client_is_created_on_first_access_and_reused(self) -> None:
        provider = Mock()
        provider.get_sas_token.return_value = "test-token"
        handler = AzureBlobPathHandler(token_provider=provider)
        with patch("iopath.common.azure_blob.azure_blob") as sdk:
            client = handler._get_client("account")
            self.assertIs(client, sdk.BlobServiceClient.return_value)
            self.assertIs(handler._get_client("account"), client)
            sdk.BlobServiceClient.assert_called_once_with(
                "https://account.blob.core.windows.net",
                credential="test-token",
            )
            provider.get_sas_token.assert_called_once_with("account")
