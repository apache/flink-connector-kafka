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
from __future__ import print_function

import glob
import io
import os
import sys

from setuptools import setup
from shutil import copy, rmtree
from xml.etree import ElementTree as ET

PACKAGE_NAME = 'apache-flink-connector-kafka'
# Source files, directories
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
POM_FILE = os.path.join(CURRENT_DIR, '../pom.xml')
README_FILE = os.path.join(CURRENT_DIR, 'README.txt')

# Generated files and directories
VERSION_FILE = os.path.join(CURRENT_DIR, 'pyflink/datastream/connectors/kafka_connector_version.py')
LIB_PATH = os.path.join(CURRENT_DIR, 'pyflink/lib')
DEPENDENCY_FILE = os.path.join(CURRENT_DIR, 'dev/dev-requirements.txt')


# Removes a file or directory if exists.
def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.isfile(file_path):
            os.remove(file_path)
        if os.path.isdir(file_path):
            rmtree(file_path)


# Reads the content of the README.txt file.
def readme_content():
    with io.open(README_FILE, 'r', encoding='utf-8') as f:
        return f.read()


# Reads the parameters used by the setup command.
# The source is the kafka_connector_version.py and the README.txt.
def setup_parameters():
    try:
        exec(open(VERSION_FILE).read())
        return locals()['__connector_version__'], locals()['__flink_dependency__'], readme_content()
    except IOError:
        print("Failed to load PyFlink version file for packaging. " +
              "'%s' not found!" % VERSION_FILE,
              file=sys.stderr)
        sys.exit(-1)


# Reads and parses the flink-connector-kafka main pom.xml.
# Based on the version data in the pom.xml prepares the pyflink dir:
#  - Generates kafka_connector_version.py
#  - Generates dev-requirements.txt
#  - Copies the flink-sql-connector-kafka*.jar to the pyflink/lib dir
def prepare_pyflink_dir():
    # source files
    pom_root = ET.parse(POM_FILE).getroot()
    flink_version = pom_root.findall(
        "./{http://maven.apache.org/POM/4.0.0}properties/" +
        "{http://maven.apache.org/POM/4.0.0}flink.version"
    )[0].text
    connector_version = pom_root.findall(
        "./{http://maven.apache.org/POM/4.0.0}version")[0].text.replace("-SNAPSHOT", ".dev0")

    flink_dependency = "apache-flink>=" + flink_version

    os.makedirs(LIB_PATH)
    connector_jar = \
        glob.glob(CURRENT_DIR + '/target/test-dependencies/flink-sql-connector-kafka*.jar')[0]
    copy(connector_jar, LIB_PATH)

    with io.open(VERSION_FILE, 'w', encoding='utf-8') as f:
        f.write('# Generated file, do not edit\n')
        f.write('__connector_version__ = "' + connector_version + '"\n')
        f.write('__flink_dependency__ = "' + flink_dependency + '"\n')

    with io.open(DEPENDENCY_FILE, 'w', encoding='utf-8') as f:
        f.write('# Generated file, do not edit\n')
        f.write(flink_dependency + '\n')


# Main
print("Python version used to package: " + sys.version)

# Python version check
if sys.version_info < (3, 7):
    print("Python versions prior to 3.7 are not supported for PyFlink.",
          file=sys.stderr)
    sys.exit(-1)

# Checks the running environment:
#  - In the connector source root directory - package preparation
#  - Otherwise - package deployment
in_flink_source = os.path.isfile("../flink-connector-kafka/src/main/" +
                                 "java/org/apache/flink/connector/kafka/source/KafkaSource.java")

# Cleans up the generated files and directories and regenerate them.
if in_flink_source:
    remove_if_exists(VERSION_FILE)
    remove_if_exists(DEPENDENCY_FILE)
    remove_if_exists(LIB_PATH)
    prepare_pyflink_dir()
    print("\nPreparing Flink Kafka connector package")

# Reads the current setup data from the kafka_connector_version.py file and the README.txt
(connector_version, flink_dependency, long_description) = setup_parameters()

print("\nConnector version: " + connector_version)
print("Flink dependency: " + flink_dependency + "\n")

if in_flink_source:
    # Removes temporary directory used by the setup tool
    remove_if_exists(PACKAGE_NAME.replace('-', '_') + '.egg-info')

# Runs the python setup
setup(
    name=PACKAGE_NAME,
    version=connector_version,
    include_package_data=True,
    url='https://flink.apache.org',
    license='https://www.apache.org/licenses/LICENSE-2.0',
    author='Apache Software Foundation',
    author_email='dev@flink.apache.org',
    python_requires='>=3.8',
    install_requires=[flink_dependency],
    description='Apache Flink Python Kafka Connector API',
    long_description=long_description,
    long_description_content_type='text/plain',
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11']
)

print("\nFlink Kafka connector package is ready\n")
