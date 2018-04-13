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

import json
from pyspark.sql import types
from os import path


class Config:
    def __init__(self, path_to_config):
        self.path = path_to_config
        path_head, _ = path.split(self.path)

        # load base config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)

        # load data_structure from base config
        with open(path.join(path_head, self.content["input"]["data_structure"])) as cfg:
            self.data_structure = json.load(cfg)

        data_structure_list = list(
            map(lambda x: (x, self.data_structure[x]), self.data_structure.keys()))
        data_structure_sorted = sorted(
            data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types.StructType(
            list(map(lambda x: types.StructField(x[0], getattr(types, x[1]["type"])()),
                     data_structure_sorted)))
