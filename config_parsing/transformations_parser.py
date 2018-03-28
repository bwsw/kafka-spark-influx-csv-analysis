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
import json
import ast
from errors import errors


class FieldTransformation:
    def __init__(self, field_name, operation):
        self.field_name = field_name  # new field name
        self.operation = operation  # SyntaxTree or string


class SyntaxTree:
    def __init__(self):
        self.operation = None
        self.children = []  # list syntax trees or strings

    def append_child(self, child):
        self.children += [child]

    def show(self, shift):
        print(" " * shift * 2 +
              "operation: {}, has {} children: ".format(self.operation, len(self.children)))
        for ch in self.children:
            if isinstance(ch, SyntaxTree):
                ch.show(shift + 1)
            else:
                print(" " * (shift + 1) * 2 + "Leaf node: ", ch)


class TransformationsParser:
    def __init__(self, transformations):
        self.transformations = transformations
        self.expanded_transformation = []  # string or FieldTransformation

    def _parse(self, args):
        result = re.search(r'(\w+)\((.*)\)', args)
        tree = SyntaxTree()
        if result is not None:
            tree.operation, arguments = result.groups()
            index, start_index, end_index = 0, 0, 0
            while index < len(arguments):
                if arguments[index] == "(":  # try find function
                    open_bracket, close_bracket = 1, 0
                    for i in range(index + 1, len(arguments)):
                        if arguments[i] == "(":
                            open_bracket += 1
                        elif arguments[i] == ")":
                            close_bracket += 1

                        if open_bracket == close_bracket:
                            end_index = i

                            child = self._parse(
                                arguments[start_index:end_index + 1])
                            tree.append_child(child)

                            start_index = index = i
                            if i + 2 < len(arguments) and arguments[i + 1] == ",":
                                start_index = i + 2  # eat 2 and start from new
                                index = i + 1
                            break
                    if open_bracket != close_bracket:
                        raise errors.IncorrectExpression(
                            "Incorrect expression: {} open brackets and {} close brackets ".
                            format(open_bracket, close_bracket))

                elif arguments[index] == ",":
                    end_index = index
                    child = self._parse(arguments[start_index:end_index])
                    tree.append_child(child)
                    start_index = end_index + 1

                index += 1

            if end_index < len(arguments) - 1:
                child = self._parse(arguments[start_index: len(arguments)])
                tree.append_child(child)

            return tree
        else:
            try:
                val = ast.literal_eval(args)
                if isinstance(val, (bool, int, float)):
                    return val
            except:
                return args

    def run(self):
        transformations = list(
            map(lambda field: field.strip(), self.transformations))
        for transformation in transformations:
            if ":" not in transformation:
                # if it's field
                self.expanded_transformation.append(transformation.strip())
            else:
                # sum/minus/div and etc operations
                operation, field_or_expression = list(map(
                    lambda t: t.strip(),
                    transformation.split(":")))
                self.expanded_transformation.append(
                    FieldTransformation(operation, self._parse(field_or_expression)))


class TransformationsParserConfig:
    def __init__(self, path_to_config):
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)
