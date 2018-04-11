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
from dispatcher.dispatcher import Dispatcher
from input.executors import Executor
from processor.processor import Processor
from output.output_writer import OutputWriter


class DispatcherTestCase(unittest.TestCase):
    @mock.patch('analysis.analysis_factory.AnalysisFactory.__init__')
    @mock.patch('pyspark.sql.session.SparkSession', autospec=True)
    def test__init__(self, mock_sparksession, mock_analysis):
        mock_context = MagicMock()
        mock_context.addFile.return_value = "test"
        mock_spark = MagicMock()
        mock_spark.sparkContext.return_value = mock_context
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark
        mock_sparksession.builder
        mock_sparksession.builder.return_value = mock_builder
        mock_analysis.return_value = None
        config = Config(os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_dispatcher.json")))
        dispatcher = Dispatcher(config)

        self.assertIsInstance(dispatcher.executor, Executor, "executor should has type Executor")
        self.assertTrue(hasattr(dispatcher.executor, "set_pipeline_processing"), "executor should has set_pipeline_processing method")

        self.assertIsInstance(dispatcher.processor, Processor, "processor should has type Processor")
        self.assertTrue(hasattr(dispatcher.processor, "get_pipeline_processing"),
                        "processor should has get_pipeline_processing method")

        self.assertIsInstance(dispatcher.writers[0], OutputWriter, "Writer should has type WriterMock")
        self.assertTrue(hasattr(dispatcher.writers[0], "get_write_lambda"), "Writer should has get_write_lambda method")
