#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

function test_module() {
    module="$FLINK_PYTHON_DIR/pyflink/$1"
    echo "test module $module"
    pytest --durations=20 ${module} $2
    if [[ $? -ne 0 ]]; then
        echo "test module $module failed"
        exit 1
    fi
}

function test_all_modules() {
    # test datastream module
    test_module "datastream"
}

# CURRENT_DIR is "flink-connector-kafka/flink-python/dev/"
CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"

# FLINK_PYTHON_DIR is "flink-connector-kafka/flink-python"
FLINK_PYTHON_DIR=$(dirname "$CURRENT_DIR")

# FLINK_SOURCE_DIR is "flink-connector-kafka"
FLINK_SOURCE_DIR=$(dirname "$FLINK_PYTHON_DIR")

# set the FLINK_TEST_LIB_DIR to "flink-connector-kafka"
export FLINK_TEST_LIBS="${FLINK_SOURCE_DIR}/flink-python/target/test-dependencies/*"

# Temporarily update the installed 'pyflink_gateway_server.py' files with the new one
# Needed only until Flink 1.19 release
echo "Checking ${FLINK_SOURCE_DIR} for 'pyflink_gateway_server.py'"
find "${FLINK_SOURCE_DIR}/flink-python" -name pyflink_gateway_server.py
find "${FLINK_SOURCE_DIR}/flink-python/.tox" -name pyflink_gateway_server.py -exec cp "${FLINK_SOURCE_DIR}/flink-python/pyflink/pyflink_gateway_server.py" {} \;

# Copy an empty flink-conf.yaml to conf/ folder, so that all Python tests on Flink 1.x can succeed.
# This needs to be changed when adding support for Flink 2.0
echo "Checking ${FLINK_SOURCE_DIR} for 'config.yaml'"
find "${FLINK_SOURCE_DIR}/flink-python" -name config.yaml

# For every occurrence of config.yaml (new YAML file since Flink 1.19), copy in the old flink-conf.yaml so that
# is used over the new config.yaml file.
#
# Because our intention is to copy `flink-conf.yaml` into the same directory as `config.yaml` and not replace it,
# we need to extract the directory from `{}` and then specify the target filename (`flink-conf.yaml`) explicitly.
# Unfortunately, `find`'s `-exec` doesn't directly support manipulating `{}`. So we use a slightly modified shell command
#
# `"${1}"` and `"${2}"` correspond to the first and second arguments after the shell command.
# In this case, `"${1}"` is the path to `flink-conf.yaml` and `"${2}"` is the path to each `config.yaml` found by `find`.
# `$(dirname "${2}")` extracts the directory part of the path to `config.yaml`, and then `/flink-conf.yaml`
# specifies the target filename within that directory.
find "${FLINK_SOURCE_DIR}/flink-python/.tox" -name config.yaml -exec sh -c 'cp "${1}" "$(dirname "${2}")/flink-conf.yaml"' _ "${FLINK_SOURCE_DIR}/flink-python/pyflink/flink-conf.yaml" {} \;

# python test
test_all_modules
