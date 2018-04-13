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
import types
import unittest

from processor.processor import Processor
from config_parsing.config import Config

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_processor.json"))
CONFIG_PATH_NUM = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_number.json"))


class ProcessorTestCase(unittest.TestCase):
    def test__init__(self):
        config = Config(CONFIG_PATH)
        p = Processor(config)
        self.assertIsInstance(p.transformation, types.LambdaType, "Processor#transformation should be a lambda object")

    def test__number__(self):
        config = Config(CONFIG_PATH_NUM)
        p = Processor(config)
        self.assertIsInstance(p.transformation, types.LambdaType, "Processor#transformation should be a lambda object")