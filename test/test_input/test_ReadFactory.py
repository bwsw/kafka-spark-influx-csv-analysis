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
from unittest import mock
from unittest.mock import MagicMock

from config_parsing.config import Config
from errors.errors import InputError
from input.executors import StreamingExecutor
from input.input_module import ReadFactory, InputConfig

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config.json"))
INCORRECT_CONFIG1_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "bad1_input_config.json"))
INCORRECT_CONFIG2_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "bad2_input_config.json"))


class ReadFactoryTestCase(unittest.TestCase):
    @mock.patch('input.input_module.KafkaUtils', autospec=True)
    @mock.patch('pyspark.sql.session.SparkSession', autospec=True)
    def test_getExecutor(self, mock_sparksession, mock_kafka_utils):
        mock_context = MagicMock()
        mock_context.addFile.return_value = "test"
        mock_spark = MagicMock()
        mock_spark.sparkContext.return_value = mock_context
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark
        mock_sparksession.builder
        mock_sparksession.builder.return_value = mock_builder
        mock_dstream = MagicMock()
        mock_dstream.map.return_value = None
        mock_kafka_utils.createDirectStream.return_value = mock_dstream

        config = Config(CONFIG_PATH)
        factory = ReadFactory(config, CONFIG_PATH)
        test_executor = factory.get_executor()

        self.assertIsInstance(test_executor, StreamingExecutor,
                              "When read csv file executor should be instance of BatchExecutable")

    def test_exception_on_error1_in_input_config(self):
        config = InputConfig(INCORRECT_CONFIG1_PATH)
        factory = ReadFactory(config, INCORRECT_CONFIG1_PATH)

        with self.assertRaises(InputError) as context:
            factory.get_executor()

        self.assertTrue("Some option was miss" in context.exception.args[0],
                        "Catch exeception, but it differs from test exception")

    def test_exception_on_error2_in_input_config(self):
        config = InputConfig(INCORRECT_CONFIG2_PATH)
        factory = ReadFactory(config, INCORRECT_CONFIG2_PATH)

        with self.assertRaises(InputError) as context:
            factory.get_executor()

        self.assertTrue("unsuported input format" in context.exception.args[0],
                        "Catch exeception, but it differs from test exception")
