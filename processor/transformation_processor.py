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

from config_parsing.transformations_parser import TransformationsParser
from config_parsing.transformations_validator import TransformationsValidator
from operations.transformation_operations import TransformationOperations
from .transformation_creator import TransformationCreator


class TransformationProcessor:
    def __init__(self, config):
        transformations_parser = TransformationsParser(config.content["processing"]["transformation"])
        transformations_parser.run()

        operations = TransformationOperations(config.content)

        self.transformations_validator = TransformationsValidator(operations, config.data_structure_pyspark)
        self.fields = self.transformations_validator.validate(transformations_parser.expanded_transformation)

        transformations_creator = TransformationCreator(config.data_structure,
                                                        transformations_parser.expanded_transformation, operations)
        row_transformations = transformations_creator.build_lambda()
        self.transformation = lambda rdd: rdd.map(row_transformations)
