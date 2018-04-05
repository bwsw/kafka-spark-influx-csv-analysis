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

from pyspark.sql.types import StructField, StructType, FloatType, LongType, StringType, BooleanType
from errors import errors
from .transformations_parser import FieldTransformation


class TransformationsValidator:
    def __init__(self, transformation_operations, data_structure_pyspark):
        self.current_fields = data_structure_pyspark
        self.transformation_operations = transformation_operations
        self.stringQuote = "'"

    def __get_field(self, field):
        try:
            return self.current_fields[field]
        except KeyError:
            raise errors.FieldNotExists(
                "Field with name '{}' does not exists".format(field))

    def _validate_syntax_tree(self, tree):
        if isinstance(tree, str):
            _tree = tree.strip()
            # case "'foobar'" - str literal
            # case  "foobar"  - alias
            if _tree.startswith(self.stringQuote) and \
                _tree.endswith(self.stringQuote):
                actual_type = StringType()
            else:  # it's field
                renamed_field = self.__get_field(_tree)
                actual_type = renamed_field.dataType
            return actual_type
        elif isinstance(tree, float):
            return FloatType()
        elif isinstance(tree, bool):
            return BooleanType()
        # Python3 primitive `int` corresponds to `long`
        elif isinstance(tree, int):
            return LongType()

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
            if isinstance(transformation, FieldTransformation):
                # string or aliasing
                if isinstance(transformation.body, str):
                    if not transformation.body.startswith(self.stringQuote) and \
                            not transformation.body.endswith(self.stringQuote):
                        field = self.__get_field(transformation.body)
                        new_fields.append(StructField(
                            transformation.name, field.dataType))
                else:  # is Syntaxtree
                    field_type = self._validate_syntax_tree(
                        transformation.body)
                    new_fields.append(StructField(
                        transformation.name, field_type))
            else:  # no transforms
                new_fields.append(self.__get_field(transformation))
        return StructType(new_fields)
