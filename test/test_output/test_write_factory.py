# Copyright 2017, bwsoft management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest

from errors import errors

from output.output_config import OutputConfig
from output.std_out_writer import StdOutWriter
from output.writer_factory import WriterFactory

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config.json"))
INCORRECT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "bad_config.json"))
CONFIG_MOUTS_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_multiple_outputs.json"))

class WriterFactoryTestCase(unittest.TestCase):
    def test_get_writers(self):
        factory = WriterFactory()
        config = OutputConfig(CONFIG_PATH)

        writers = factory.get_writers(config, list(), list())
        self.assertIsInstance(writers[0], StdOutWriter, "Writer should be instance of StdOutWriter")

    def test_unsupported_output_format_exception_get_writers(self):
        factory = WriterFactory()
        config = OutputConfig(INCORRECT_CONFIG_PATH)

        with self.assertRaises(errors.UnsupportedOutputFormat):
            factory.get_writers(config, list(), list())

    def test_multiple_outputs(self):
        factory = WriterFactory()
        config = OutputConfig(CONFIG_MOUTS_PATH)

        writers = factory.get_writers(config, list(), list())
        self.assertIsInstance(writers[0], StdOutWriter, "Writer should be instance of StdOutWriter")
        self.assertIsInstance(writers[1], StdOutWriter, "Writer should be instance of StdOutWriter")
