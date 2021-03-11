#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates. All rights reserved.
set -ex

mkdir -p packaging/out

version=$(python -c "exec(open('iopath/version.py').read()); print(__version__)")
# build_version=$version.post$(date +%Y%m%d)

export BUILD_VERSION=$version

if [[ "3.6" == "$PYTHON_VERSION" ]]
then
    export DATACLASSES_CONSTRAINT="- dataclasses"
else
    export DATACLASSES_CONSTRAINT=""
fi

conda build -c defaults -c conda-forge --no-anaconda-upload --python "$PYTHON_VERSION" --output-folder packaging/out packaging/iopath
