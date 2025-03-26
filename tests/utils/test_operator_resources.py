#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

from airflow.utils.operator_resources import Resources


class TestResources(unittest.TestCase):
    def test_resource_eq(self):
        r = Resources(cpus=0.1, ram=2048)
        assert r not in [{}, [], None]
        assert r == r

        r2 = Resources(cpus=0.1, ram=2048)
        assert r == r2
        assert r2 == r

        r3 = Resources(cpus=0.2, ram=2048)
        assert r != r3
