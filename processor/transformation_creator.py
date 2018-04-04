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

from config_parsing.transformations_parser import FieldTransformation
import re
import ast


class TransformationCreator:
    def __init__(self, data_structure, parsed_transformation, transformation_operations):
        self.parsed_transformation = parsed_transformation
        self.mapping = dict(
            map(lambda x: (x, data_structure[x]["index"]), data_structure.keys()))
        self.transformation_operations = transformation_operations
        self.stringQuote = "'"

    def __generate_params_list(self, children, row):
        args = []
        for ch in children:
            if isinstance(ch, str):
                if ch.startswith(self.stringQuote) and \
                        ch.endswith(self.stringQuote):
                    args.append(ch)
                    continue
                try:
                    ch_typed = ast.literal_eval(ch)
                    if isinstance(ch_typed, (bool, int, float, str)):
                        args.append(ch_typed)
                except:
                    args.append(row[self.mapping[ch]]
                                if ch in self.mapping.keys() else int(ch))
            else:
                operation = self.transformation_operations.operations_dict[ch.operation].func
                args.append(
                    operation(*self.__generate_params_list(ch.children, row)))
        return args

    def _get_column_value_lambda(self, index):
        return lambda row: row[index]

    def _make_operation_lambda(self, syntax_tree):
        operation = self.transformation_operations.operations_dict[syntax_tree.operation].func

        return lambda row: operation(*self.__generate_params_list(syntax_tree.children, row))

    def build_lambda(self):
        lambdas = []
        for exp_tr in self.parsed_transformation:
            # always FieldTransformation
            if isinstance(exp_tr, FieldTransformation):
                if isinstance(exp_tr.body, str):
                    if exp_tr.body.startswith(self.stringQuote) and \
                            exp_tr.body.endswith(self.stringQuote):
                        lambdas.append(
                            self._get_column_value_lambda(exp_tr.body))
                    else:
                        try:
                            v = ast.literal_eval(exp_tr.body)
                            if isinstance(v, (bool, int, float)):
                                lambdas.append(
                                    self._get_column_value_lambda(exp_tr.body))
                        except:
                            lambdas.append(self._get_column_value_lambda(
                                self.mapping[exp_tr.body]))
                elif isinstance(exp_tr.body, (bool, int, float)):
                    lambdas.append(exp_tr.body)
                else:
                    syntax_tree = exp_tr.body
                    lambdas.append(self._make_operation_lambda(syntax_tree))
            else:
                lambdas.append(self._get_column_value_lambda(
                    self.mapping[exp_tr]))
        return lambda row: (tuple(map(lambda x: x(row), lambdas)))
