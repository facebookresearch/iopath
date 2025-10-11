# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import io
import os
import sys
import unittest
from unittest.mock import patch

import google.cloud.storage as google_cloud_storage
from iopath.common.file_io import PathManager
from torchuv.utils import GcsPathHandler


@unittest.skipIf(not google_cloud_storage, "Requires google-cloud-storage install")
class TestGCS(unittest.TestCase):
    gcs_auth = True
    skip_gcs_auth_required_tests_message = (
        "Provide an gcs project and bucket you are"
        + "authorised against, then set the gcs_auth flag to True"
    )

    def run(self, result=None):
        with patch("iopath.common.event_logger.EventLogger.log_event"):
            super(TestGCS, self).run(result)

    #############################################
    # Shared
    #############################################
    @classmethod
    def setUpClass(cls):
        # NOTE: user should change this location.
        cls.gcs_bucket = "TEST_BUCKET_NAME_REPLACE_ME"
        # NOTE: user should change this to a valid bucket path that is accessible.
        cls.gcs_rel_path = "TEST_REL_PATH_REPLACE_ME"
        cls.gcs_full_path = f"gs://{cls.gcs_bucket}/{cls.gcs_rel_path}"

        cls.gcs_pathhandler = GcsPathHandler()
        cls.pathmanager = PathManager()
        cls.pathmanager.register_handler(cls.gcs_pathhandler)

        # Hack to make the test cases ordered.
        # https://stackoverflow.com/questions/4005695/changing-order-of-unit-tests-in-python
        cls.hack = unittest.TestLoader.sortTestMethodsUsing
        unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: y > x

    @classmethod
    def tearDownClass(cls):
        # Undo the hack
        unittest.TestLoader.sortTestMethodsUsing = cls.hack

        if not cls.gcs_auth:
            return

        # Recursive deletion is not implemented,
        # so let's delete each file and directory.

        # Delete all files
        cls.gcs_pathhandler._rm(
            "/".join([cls.gcs_full_path, "dir1", "f1_write_string"])
        )
        cls.gcs_pathhandler._rm("/".join([cls.gcs_full_path, "dir1", "f2_write_bytes"]))
        cls.gcs_pathhandler._rm(
            "/".join([cls.gcs_full_path, "dir2", "f1_write_string_from_local"])
        )
        cls.gcs_pathhandler._rm(
            "/".join([cls.gcs_full_path, "dir2", "f2_write_bytes_from_local"])
        )
        cls.gcs_pathhandler._rm(
            "/".join([cls.gcs_full_path, "dir2", "f3_write_string_from_local"])
        )
        cls.gcs_pathhandler._rm(
            "/".join([cls.gcs_full_path, "dir2", "f4_write_bytes_from_local"])
        )
        cls.gcs_pathhandler._rm("/".join([cls.gcs_full_path, "dir1", "alphabet"]))
        cls.gcs_pathhandler._rm("/".join([cls.gcs_full_path, "dir1", "large_text"]))

        # Delete all directories.
        cls.gcs_pathhandler._rm("/".join([cls.gcs_full_path, "dir3", "dir4/"]))
        for i in (1, 2, 3):
            cls.gcs_pathhandler._rm("/".join([cls.gcs_full_path, f"dir{i}/"]))

        assert cls.gcs_pathhandler._ls(cls.gcs_full_path) == []

    def test_00_supported_prefixes(self):
        supported_prefixes = self.gcs_pathhandler._get_supported_prefixes()
        self.assertEqual(supported_prefixes, ["gs://", "gcs://"])

    # # Require GCS Authentication ====>

    #############################################
    # Organization of GCS setup
    # dir1/
    #   f1 <- small (via open)
    #   f2 <- large checkpoint file (via open)
    # dir2/
    #   f3 <- small (via copy(), from dir1)
    #   f4 <- large checkpoint file (via copy_from_local)
    # dir3/
    #   dir4/
    #############################################

    #############################################
    # auth
    # Just check that client loads properly
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_01_add_client_to_handler(self):
        self.gcs_pathhandler._get_client()

    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_02_mkdirs_must_end_with_slash(self):
        with self.assertRaises(AssertionError):
            self.gcs_pathhandler._mkdirs("/".join([self.gcs_full_path, "fail"]))

    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_03_mkdirs(self):
        # dir{1,2,3} in BASE
        for i in (1, 2, 3):
            self.gcs_pathhandler._mkdirs("/".join([self.gcs_full_path, f"dir{i}/"]))

        # Make a nested directory in dir3
        self.gcs_pathhandler._mkdirs("/".join([self.gcs_full_path, "dir3/dir4/"]))

    #############################################
    # open (w/wb)
    #   +f1
    #   +f2
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_04_open_write_mode(self):
        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir1", "f1_write_string"]), "w"
        ) as f:
            f.write("This is a test of writing a string.")

        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir1", "f2_write_bytes"]), "wb"
        ) as f:
            f.write(b"This is a test of writing bytes.")

        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir1", "f1_write_string"]), "w"
        ) as f:
            f.write("This is a test of overwriting a string.")

        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir2", "f3_write_string"]), "w"
        ) as f:
            f.write("This is a test of writing a string that I'll use to test rm.")

    #############################################
    # open (r/rb)
    #   read f1
    #   read f2
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_05_open_read_mode(self):
        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir1", "f1_write_string"]), "r"
        ) as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")

        with self.gcs_pathhandler._open(
            "/".join([self.gcs_full_path, "dir1", "f2_write_bytes"]), "rb"
        ) as f:
            self.assertEqual(f.read(), b"This is a test of writing bytes.")

    #############################################
    # isdir / isfile / exists
    #   test dir{1,2,3,4}
    #   test f{1,2}
    #   test nonexistants
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_06_exists(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.gcs_pathhandler._exists("/".join([self.gcs_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.gcs_pathhandler._exists("/".join([self.gcs_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertTrue(
            self.gcs_pathhandler._exists(
                "/".join([self.gcs_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertTrue(
            self.gcs_pathhandler._exists("/".join([self.gcs_full_path, "dir1/"]))
        )

    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_07_isdir(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.gcs_pathhandler._isdir("/".join([self.gcs_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.gcs_pathhandler._isdir("/".join([self.gcs_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertFalse(
            self.gcs_pathhandler._isdir(
                "/".join([self.gcs_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertTrue(
            self.gcs_pathhandler._isdir("/".join([self.gcs_full_path, "dir1/"]))
        )

    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_08_isfile(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.gcs_pathhandler._isfile("/".join([self.gcs_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.gcs_pathhandler._isfile("/".join([self.gcs_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertTrue(
            self.gcs_pathhandler._isfile(
                "/".join([self.gcs_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertFalse(
            self.gcs_pathhandler._isfile("/".join([self.gcs_full_path, "dir1/"]))
        )

    #############################################
    # ls
    #   ls dir{1,2,3,4}
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_10_ls(self):
        # Path does not exist (if file)
        self.assertEqual(
            [], self.gcs_pathhandler._ls("/".join([self.gcs_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertEqual(
            [], self.gcs_pathhandler._ls("/".join([self.gcs_full_path, "FAIL/"]))
        )
        # Path exists (is dir)
        self.assertEqual(
            {
                "/".join([self.gcs_rel_path, "dir1", "f1_write_string"]),
                "/".join([self.gcs_rel_path, "dir1", "f2_write_bytes"]),
            },
            set(self.gcs_pathhandler._ls("/".join([self.gcs_full_path, "dir1/"]))),
        )

    #############################################
    # rm
    #   rm f3
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_11_rm(self):
        path = "/".join([self.gcs_full_path, "dir2", "f3_write_string"])
        self.assertTrue(self.gcs_pathhandler._exists(path))
        self.assertTrue(self.gcs_pathhandler._isfile(path))
        self.assertFalse(self.gcs_pathhandler._isdir(path))

        self.gcs_pathhandler._rm(path)

        self.assertFalse(self.gcs_pathhandler._exists(path))
        self.assertFalse(self.gcs_pathhandler._isfile(path))
        self.assertFalse(self.gcs_pathhandler._isdir(path))

    #############################################
    # get_local_path
    #   Retrieve f{1,2}
    #   Check file contents.
    #############################################
    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_12_get_local_path(self):
        gcs_path_f1 = "/".join([self.gcs_full_path, "dir1", "f1_write_string"])
        gcs_path_f2 = "/".join([self.gcs_full_path, "dir1", "f2_write_bytes"])

        local_path_f1 = self.gcs_pathhandler._get_local_path(gcs_path_f1)
        local_path_f2 = self.gcs_pathhandler._get_local_path(gcs_path_f2)

        with open(local_path_f1, "r") as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")
        with open(local_path_f2, "rb") as f:
            self.assertEqual(f.read(), b"This is a test of writing bytes.")

    @unittest.skipIf(not gcs_auth, skip_gcs_auth_required_tests_message)
    def test_13_get_local_path_idempotent(self):
        """
        Call _get_local_path multiple times.
        Check that we keep returning the same cached copy instead of redownloading.
        """
        gcs_path_f1 = "/".join([self.gcs_full_path, "dir1", "f1_write_string"])

        REPEATS = 3
        local_paths = [
            self.gcs_pathhandler._get_local_path(gcs_path_f1) for _ in range(REPEATS)
        ]
        for local_path in local_paths[1:]:
            self.assertEqual(local_path, local_paths[0])

        with open(local_paths[0], "r") as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")
