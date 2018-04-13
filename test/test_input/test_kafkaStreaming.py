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

from unittest import TestCase, skip
from errors.errors import KafkaConnectError
from input.executors import StreamingExecutor
from input.input_module import KafkaStreaming


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


@skip("The method of optimal testing is not determined. Requires server start with kafka")
class TestKafkaStreaming(TestCase):
    def test_getExecutor(self):
        config = TestConfig({"server": "localhost", "port": 29092, "topic": "data", "batchDuration": 4, "sep": ","})
        test_read = KafkaStreaming(config.content)
        test_executor = test_read.get_streaming_executor()
        self.assertIsInstance(test_executor, StreamingExecutor,
                              "When ruse kafka streaming executor should be instance of StreamingExecutor")

        config = TestConfig({"server": "localhost", "port": 29091, "topic": "data", "batchDuration": 4, "sep": ","})
        with self.assertRaises(KafkaConnectError) as context:
            _ = KafkaStreaming(config.content)
        self.assertTrue("Kafka error" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")
