# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import io
import unittest
from unittest.mock import patch

from iopath.common.file_io import PathManager
from iopath.common.s3 import S3ChunkReadIO, S3PathHandler


try:
    import boto3
except ImportError:
    boto3 = None

test_bucket = "TEST_BUCKET_NAME_REPLACE_ME"
test_rel_path = "TEST_REL_PATH_REPLACE_ME"


def test_bucket_defined():
    return (
        test_bucket != "TEST_BUCKET_NAME_REPLACE_ME"
        and test_rel_path != "TEST_REL_PATH_REPLACE_ME"
    )


@unittest.skipIf(not boto3, "Requires boto3 install")
@unittest.skipIf(
    test_bucket == "TEST_BUCKET_NAME_REPLACE_ME", "Test Bucket not specified."
)
@unittest.skipIf(
    test_rel_path == "TEST_REL_PATH_REPLACE_ME", "Test relative path not specified."
)
class TestsS3(unittest.TestCase):
    s3_auth = test_bucket == test_bucket_defined()
    skip_s3_auth_required_tests_message = (
        "Provide an s3 project and bucket you are"
        + "authorised against, then set the s3_auth flag to True"
    )

    def run(self, result=None):
        with patch("iopath.common.event_logger.EventLogger.log_event"):
            super(TestsS3, self).run(result)

    #############################################
    # Shared
    #############################################
    @classmethod
    def setUpClass(cls):
        # NOTE: user should change this location.
        cls.s3_bucket = test_bucket
        # NOTE: user should change this to a valid bucket path that is accessible.
        cls.s3_rel_path = test_rel_path
        cls.s3_full_path = "s3://" + cls.s3_bucket + "/" + cls.s3_rel_path
        cls.s3_pathhandler = S3PathHandler()
        cls.pathmanager = PathManager()
        cls.pathmanager.register_handler(cls.s3_pathhandler)

        # Hack to make the test cases ordered.
        # https://stackoverflow.com/questions/4005695/changing-order-of-unit-tests-in-python
        cls.hack = unittest.TestLoader.sortTestMethodsUsing
        unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: y > x

    @classmethod
    def tearDownClass(cls):
        # Undo the hack
        unittest.TestLoader.sortTestMethodsUsing = cls.hack

        if not cls.s3_auth:
            return

        # Recursive deletion is not implemented,
        # so let's delete each file and directory.

        # Delete all files
        cls.s3_pathhandler._rm("/".join([cls.s3_full_path, "dir1", "f1_write_string"]))
        cls.s3_pathhandler._rm("/".join([cls.s3_full_path, "dir1", "f2_write_bytes"]))
        cls.s3_pathhandler._rm(
            "/".join([cls.s3_full_path, "dir2", "f1_write_string_from_local"])
        )
        cls.s3_pathhandler._rm(
            "/".join([cls.s3_full_path, "dir2", "f2_write_bytes_from_local"])
        )
        cls.s3_pathhandler._rm(
            "/".join([cls.s3_full_path, "dir2", "f3_write_string_from_local"])
        )
        cls.s3_pathhandler._rm(
            "/".join([cls.s3_full_path, "dir2", "f4_write_bytes_from_local"])
        )
        cls.s3_pathhandler._rm("/".join([cls.s3_full_path, "dir1", "alphabet"]))
        cls.s3_pathhandler._rm("/".join([cls.s3_full_path, "dir1", "large_text"]))

        # Delete all directories.
        cls.s3_pathhandler._rm("/".join([cls.s3_full_path, "dir3", "dir4/"]))
        for i in (1, 2, 3):
            cls.s3_pathhandler._rm("/".join([cls.s3_full_path, f"dir{i}/"]))

        assert cls.s3_pathhandler._ls(cls.s3_full_path) == []

    #############################################
    # Up here, test class attributes,
    # and helpers that don't require S3 access.
    #############################################
    def test_00_supported_prefixes(self):
        supported_prefixes = self.s3_pathhandler._get_supported_prefixes()
        self.assertEqual(supported_prefixes, ["s3://"])

    # # Require S3 Authentication ====>

    #############################################
    # Organization of s3 setup
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
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_01_add_client_to_handler(self):
        self.s3_pathhandler._get_client(
            "/".join([self.s3_full_path, "path", "file.txt"])
        )
        # self.assertTrue(isinstance(self.s3_pathhandler.client, botocore.client.S3)) # TODO

    # TODO: make sure that the error message displays properly if authentication is messed up.

    #############################################
    # mkdirs
    # Set up the dirs
    # (in BASE)
    #   +dir1
    #   +dir2
    #   +dir3
    #   +dir4
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_02_mkdirs_must_end_with_slash(self):
        with self.assertRaises(AssertionError):
            self.s3_pathhandler._mkdirs("/".join([self.s3_full_path, "fail"]))

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_03_mkdirs(self):
        # dir{1,2,3} in BASE
        for i in (1, 2, 3):
            self.s3_pathhandler._mkdirs("/".join([self.s3_full_path, f"dir{i}/"]))

        # Make a nested directory in dir3
        self.s3_pathhandler._mkdirs("/".join([self.s3_full_path, "dir3/dir4/"]))

    #############################################
    # open (w/wb)
    #   +f1
    #   +f2
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_04_open_write_mode(self):
        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "f1_write_string"]), "w"
        ) as f:
            f.write("This is a test of writing a string.")

        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "f2_write_bytes"]), "wb"
        ) as f:
            f.write(b"This is a test of writing bytes.")

        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "f1_write_string"]), "w"
        ) as f:
            f.write("This is a test of overwriting a string.")

    #############################################
    # open (r/rb)
    #   read f1
    #   read f2
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_05_open_read_mode(self):
        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "f1_write_string"]), "r"
        ) as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")

        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "f2_write_bytes"]), "rb"
        ) as f:
            self.assertEqual(f.read(), b"This is a test of writing bytes.")

    #############################################
    # isdir / isfile / exists
    #   test dir{1,2,3,4}
    #   test f{1,2}
    #   test nonexistants
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_06_exists(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.s3_pathhandler._exists("/".join([self.s3_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.s3_pathhandler._exists("/".join([self.s3_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertTrue(
            self.s3_pathhandler._exists(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertTrue(
            self.s3_pathhandler._exists("/".join([self.s3_full_path, "dir1/"]))
        )

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_07_isdir(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.s3_pathhandler._isdir("/".join([self.s3_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.s3_pathhandler._isdir("/".join([self.s3_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertFalse(
            self.s3_pathhandler._isdir(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertTrue(
            self.s3_pathhandler._isdir("/".join([self.s3_full_path, "dir1/"]))
        )

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_08_isfile(self):
        # Path does not exist (if file)
        self.assertFalse(
            self.s3_pathhandler._isfile("/".join([self.s3_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertFalse(
            self.s3_pathhandler._isfile("/".join([self.s3_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertTrue(
            self.s3_pathhandler._isfile(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"])
            )
        )
        # Path exists (is dir)
        self.assertFalse(
            self.s3_pathhandler._isfile("/".join([self.s3_full_path, "dir1/"]))
        )

    #############################################
    # copy
    #   copy f1 -> f3
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_09_copy(self):
        self.assertTrue(
            self.s3_pathhandler._copy(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"]),
                "/".join([self.s3_full_path, "dir2", "f3_write_string"]),
            )
        )

    #############################################
    # ls
    #   ls dir{1,2,3,4}
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_10_ls(self):
        # Path does not exist (if file)
        self.assertEqual(
            [], self.s3_pathhandler._ls("/".join([self.s3_full_path, "dir1", "FAIL"]))
        )
        # Path does not exist (if dir)
        self.assertEqual(
            [], self.s3_pathhandler._ls("/".join([self.s3_full_path, "FAIL/"]))
        )
        # Path exists (is file)
        self.assertEqual(
            ["/".join([self.s3_rel_path, "dir1", "f1_write_string"])],
            self.s3_pathhandler._ls(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"])
            ),
        )
        # Path exists (is dir)
        self.assertEqual(
            {
                "/".join([self.s3_rel_path, "dir1/"]),
                "/".join([self.s3_rel_path, "dir1", "f1_write_string"]),
                "/".join([self.s3_rel_path, "dir1", "f2_write_bytes"]),
            },
            set(self.s3_pathhandler._ls("/".join([self.s3_full_path, "dir1/"]))),
        )

    #############################################
    # rm
    #   rm f3
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_11_rm(self):
        path = "/".join([self.s3_full_path, "dir2", "f3_write_string"])
        self.assertTrue(self.s3_pathhandler._exists(path))
        self.assertTrue(self.s3_pathhandler._isfile(path))
        self.assertFalse(self.s3_pathhandler._isdir(path))

        self.s3_pathhandler._rm(path)

        self.assertFalse(self.s3_pathhandler._exists(path))
        self.assertFalse(self.s3_pathhandler._isfile(path))
        self.assertFalse(self.s3_pathhandler._isdir(path))

    #############################################
    # get_local_path
    #   Retrieve f{1,2}
    #   Check file contents.
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_12_get_local_path(self):
        s3_path_f1 = "/".join([self.s3_full_path, "dir1", "f1_write_string"])
        s3_path_f2 = "/".join([self.s3_full_path, "dir1", "f2_write_bytes"])

        local_path_f1 = self.s3_pathhandler._get_local_path(s3_path_f1)
        local_path_f2 = self.s3_pathhandler._get_local_path(s3_path_f2)

        with open(local_path_f1, "r") as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")
        with open(local_path_f2, "rb") as f:
            self.assertEqual(f.read(), b"This is a test of writing bytes.")

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_13_get_local_path_idempotent(self):
        """
        Call _get_local_path multiple times.
        Check that we keep returning the same cached copy instead of redownloading.
        """
        s3_path_f1 = "/".join([self.s3_full_path, "dir1", "f1_write_string"])

        REPEATS = 3
        local_paths = [
            self.s3_pathhandler._get_local_path(s3_path_f1) for _ in range(REPEATS)
        ]
        for local_path in local_paths[1:]:
            self.assertEqual(local_path, local_paths[0])

        with open(local_paths[0], "r") as f:
            self.assertEqual(f.read(), "This is a test of overwriting a string.")

    ##############################################
    # copy_from_local
    #   Upload local copies of f1, f2 -> f3, f4.
    # Check contents via open(), and via another get_local_path
    ##############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_14_copy_from_local(self):
        s3_src_path_f1 = "/".join([self.s3_full_path, "dir1", "f1_write_string"])
        s3_src_path_f2 = "/".join([self.s3_full_path, "dir1", "f2_write_bytes"])

        local_path_f1 = self.s3_pathhandler._get_local_path(s3_src_path_f1)
        local_path_f2 = self.s3_pathhandler._get_local_path(s3_src_path_f2)

        s3_dst_path_f1 = "/".join(
            [self.s3_full_path, "dir2", "f1_write_string_from_local"]
        )
        s3_dst_path_f2 = "/".join(
            [self.s3_full_path, "dir2", "f2_write_bytes_from_local"]
        )

        self.assertTrue(
            self.s3_pathhandler._copy_from_local(local_path_f1, s3_dst_path_f1)
        )
        self.assertTrue(
            self.s3_pathhandler._copy_from_local(local_path_f2, s3_dst_path_f2)
        )

    #############################################
    # symlink
    #   should fail
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_15_symlink(self):
        s3_src_path_f1 = "/".join([self.s3_full_path, "dir1", "f1_write_string"])
        s3_dst_path_f1 = "/".join(
            [self.s3_full_path, "dir2", "f1_write_string_symlink"]
        )
        with self.assertRaises(NotImplementedError):
            self.s3_pathhandler._symlink(s3_src_path_f1, s3_dst_path_f1)

    #############################################
    # PathManager
    #   Minimal ls() test with PathManager
    #############################################
    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_16_pathmanager(self):
        # Path exists (is file)
        self.assertEqual(
            ["/".join([self.s3_rel_path, "dir1", "f1_write_string"])],
            self.pathmanager.ls(
                "/".join([self.s3_full_path, "dir1", "f1_write_string"])
            ),
        )

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_17_chunk_reading(self):
        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "alphabet"]), "w"
        ) as f:
            f.write("abcdefghijklmnopqrstuvwxyz")

        session = boto3.Session()
        client = session.client("s3")

        reader = S3ChunkReadIO(
            client,
            bucket=self.s3_bucket,
            key="/".join([self.s3_rel_path, "dir1", "alphabet"]),
            chunk_size=6,
        )

        reader.seek(2)

        self.assertEqual(reader.read(4).decode("utf-8"), "cdef")
        self.assertEqual(reader.read(13).decode("utf-8"), "ghijklmnopqrs")
        self.assertEqual(reader.read().decode("utf-8"), "tuvwxyz")

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_19_test_chunk_read_text(self):
        large_text = "HELLO!\nTHIS IS A TEST\nTHANKS!" * 10 * 1024
        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "large_text"]), "w"
        ) as wf:
            wf.write(large_text)
            wf.close()

        for test_chunk_size in [4 * 1024, 8 * 1024, 12 * 1024, 16 * 1024, 1024 * 1024]:
            with self.s3_pathhandler._open(
                "/".join([self.s3_full_path, "dir1", "large_text"]),
                "r",
                read_chunk_size=test_chunk_size,
            ) as rf:
                delimiter = "\n"
                expected_lines = large_text.split(delimiter)[:-1]
                for line in expected_lines:
                    s = rf.readline()
                    self.maxDiff = None
                    self.assertEqual(s, line + delimiter)
                rf.close()

    @unittest.skipIf(not s3_auth, skip_s3_auth_required_tests_message)
    def test_20_test_chunk_read_binary(self):
        large_text = "HELLO!\nTHIS IS A TEST\nTHANKS!" * 10 * 1024
        with self.s3_pathhandler._open(
            "/".join([self.s3_full_path, "dir1", "large_text_binary"]), "wb"
        ) as wf:
            wf.write(large_text.encode())
            wf.close()

        for test_chunk_size in [4 * 1024, 8 * 1024, 12 * 1024, 16 * 1024, 1024 * 1024]:
            buffer = io.BytesIO()
            with self.s3_pathhandler._open(
                "/".join([self.s3_full_path, "dir1", "large_text_binary"]),
                "rb",
                read_chunk_size=test_chunk_size,
            ) as rf:
                while True:
                    chunk = rf.read(6 * 1024)
                    if len(chunk) > 0:
                        buffer.write(chunk)
                    else:
                        break
                self.assertEqual(large_text.encode(), buffer.getvalue())
                rf.close()
