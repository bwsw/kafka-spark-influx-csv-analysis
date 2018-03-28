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

import re

from pyspark.sql.types import StructField, StructType, FloatType, LongType
from errors import errors
from .transformations_parser import FieldTransformation


class TransformationsValidator:
    def __init__(self, transformation_operations, data_structure_pyspark):
        self.current_fields = data_structure_pyspark
        self.transformation_operations = transformation_operations

    def __get_field(self, field):
        try:
            return self.current_fields[field]
        except KeyError:
            raise errors.FieldNotExists(
                "Field with name '{}' does not exists".format(field))

    def _validate_syntax_tree(self, tree):
        if isinstance(tree, str):
            f = tree.strip()
            if re.search('^(\d+)$', f) is not None:  # it's long number
                actual_type = LongType()
            elif re.search('^(\d+\.\d+)$', f) is not None:  # it's float number
                actual_type = FloatType()
            else:  # it's field
                renamed_field = self.__get_field(f)
                actual_type = renamed_field.dataType
            return actual_type

        operation = self.transformation_operations.operations_dict.get(
            tree.operation, None)

        if operation is None:
            raise errors.OperationNotSupportedError(
                "Operation '{}' is not supported.".format(tree.operation))

        if operation.op_count != len(tree.children):
            raise errors.IncorrectArgumentsAmountForOperationError(
                "Operation '{}' expects {} arguments.".format(operation, operation.op_count))

        return operation.result_type(list(map(lambda ch: self._validate_syntax_tree(ch), tree.children)))

    def validate(self, transformations):
        new_fields = []
        for transformation in transformations:
            if isinstance(transformation, FieldTransformation):  # it's transformed name
                if isinstance(transformation.operation, str):  # it's rename
                    field = self.__get_field(transformation.operation)
                    new_fields.append(StructField(
                        transformation.field_name, field.dataType))
                else:  # is Syntaxtree
                    field_type = self._validate_syntax_tree(
                        transformation.operation)
                    new_fields.append(StructField(
                        transformation.field_name, field_type))
            else:  # no transforms
                new_fields.append(self.__get_field(transformation))
        return StructType(new_fields)
