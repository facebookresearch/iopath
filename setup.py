#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

from os import path

from setuptools import find_packages, setup


def get_version():
    init_py_path = path.join(
        path.abspath(path.dirname(__file__)), "iopath", "__init__.py"
    )
    init_py = open(init_py_path, "r").readlines()
    version_line = [line.strip() for line in init_py if line.startswith("__version__")][
        0
    ]
    version = version_line.split("=")[-1].strip().strip("'\"")

    return version


setup(
    name="iopath",
    version=get_version(),
    author="FAIR",
    license="MIT licensed, as found in the LICENSE file",
    url="https://github.com/facebookresearch/iopath",
    description="A library for providing I/O abstraction.",
    python_requires=">=3.6",
    install_requires=["tqdm", "portalocker"],
    packages=find_packages(exclude=("tests",)),
)
