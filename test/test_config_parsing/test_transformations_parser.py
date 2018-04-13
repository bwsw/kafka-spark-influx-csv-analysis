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

from errors import errors
from config_parsing.transformations_parser import \
    TransformationsParser, TransformationsParserConfig, SyntaxTree, FieldTransformation

CONFIG_PATH = os.path.join(os.path.dirname(
    __file__), os.path.join("..", "data", "config.json"))

stub = {
    "add": [2, 3],
    "first_mult": [1, 3],
    "second_mult": [
        1,
        [2, 3]
    ],
    "run_test": [
        {  # 0
            "type": str,
            "field_name": "source_ip"
        },
        {  # 1
            # skip
        },
        {  # 2
            "type": SyntaxTree,
            "field_name": "src_country"
        },
        {  # 3
            "type": SyntaxTree,
            "field_name": "traffic"
        }
    ],
    "config": "'input.options.port'"
}


class TransformationsParserTest(unittest.TestCase):
    def test__parse_field(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        result = parser._parse("sample_rating", True)
        self.assertIsInstance(
            result, str, "Result should be instance of string")
        self.assertEqual(result, "sample_rating",
                         "Value this leaf node should be 'sample_rating'")

    def test__parse_transformation_types(self):
        parser = TransformationsParser([])
        p = parser._parse("1", True)
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of int")
        self.assertIsInstance(p.children[0], int,
                              "Result should be an instance of int")
        p = parser._parse("1.0", True)
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of float")
        self.assertIsInstance(p.children[0], float,
                              "Result should be an instance of int")
        p = parser._parse("False", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of bool")
        self.assertIsInstance(p.children[0], bool,
                              "Result should be an instance of int")
        p = parser._parse("True", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of bool")
        self.assertIsInstance(p.children[0], bool,
                              "Result should be an instance of int")
        p = parser._parse("'Fo,o'", True)                           
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of str")
        self.assertIsInstance(p.children[0], str,
                              "Result should be an instance of int")
        p = parser._parse("'Foo\\'Bar'", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of str")
        self.assertIsInstance(p.children[0], str,
                              "Result should be an instance of int")
        p = parser._parse("'Bar'", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of str")
        self.assertIsInstance(p.children[0], str,
                              "Result should be an instance of int")
        p = parser._parse("4E+8", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of float")
        self.assertIsInstance(p.children[0], float,
                              "Result should be an instance of int")
        p = parser._parse("'Foo\\\"bar'", True)                              
        self.assertIsInstance(p, SyntaxTree,
                              "Result should be an instance of str")
        self.assertIsInstance(p.children[0], str,
                                  "Result should be an instance of int")
        p = parser._parse("sample_rating",True)                              
        self.assertIsInstance(p, str,
                              "Result should be an instance of str")

    def test__parse_simple_operation(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        expression = "add({})".format(",".join(str(i) for i in stub["add"]))

        result = parser._parse(expression, True)
        self.assertIsInstance(result, SyntaxTree,
                              "Result should be instance of SyntaxTree")
        self.assertEqual(result.operation, "add", "Operation should be 'add'")
        self.assertEqual(len(result.children), 2, "Should have 2 children")

        for index in range(0, 2):
            self.assertIsInstance(result.children[index], int,
                                  "children[{}] should be instance of Leaf".format(index))
            self.assertEqual(result.children[index], stub["add"][index],
                             "Add {} argument should be {}".format(index, stub["add"][index]))

    def test__parse_config_operation(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        expression = "config({})".format(stub["config"])

        result = parser._parse(expression, True)

        self.assertIsInstance(result, SyntaxTree,
                               "Result should be instance of SyntaxTree")
        self.assertEqual(result.operation, "config", "Operation should be 'config'")
        self.assertEqual(len(result.children), 1, "Should have 1 children")

        self.assertIsInstance(result.children[0], str,
                                   "children[{}] should be instance of Leaf".format(0))

    def test__parse_nested_operations(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        expression = "sub(mul({}),mul({},add({})))".format(
            ",".join(str(i) for i in stub["first_mult"]),
            str(stub["second_mult"][0]),
            ",".join(str(i) for i in stub["second_mult"][1]))
        result = parser._parse(expression, True)
        self.assertIsInstance(result, SyntaxTree,
                              "Result should be instance of SyntaxTree")
        self.assertEqual(result.operation, "sub",
                         "Operation should be 'sub'")
        self.assertEqual(len(result.children), 2, "Should have 2 children")

        # Check first child # mul(1,3)
        first_mult = result.children[0]  # mul(1,3)

        self.assertIsInstance(first_mult, SyntaxTree,
                              "Result should be instance of SyntaxTree")
        self.assertEqual(first_mult.operation, "mul",
                         "Operation should be 'mul'")
        self.assertEqual(len(first_mult.children), 2, "Should have 2 children")

        for index in range(0, 2):
            self.assertIsInstance(
                first_mult.children[index],
                int,
                "children[{}] should be instance of str".format(index))
            self.assertEqual(
                first_mult.children[index],
                stub["first_mult"][index],
                "Mult {} argument should be {}".format(index, str(stub["first_mult"][index])))

        # Check second child mult(1,sum(2,3))
        second_mult = result.children[1]
        self.assertIsInstance(second_mult, SyntaxTree,
                              "Result should be instance of SyntaxTree")
        self.assertEqual(second_mult.operation, "mul",
                         "Operation should be 'mul'")
        self.assertEqual(len(second_mult.children),
                         2, "Should have 2 children")

        # second_mult[0] should be 1
        self.assertIsInstance(
            second_mult.children[0],
            int,
            "children[{}] should be instance of str".format(0))
        self.assertEqual(
            second_mult.children[0],
            stub["second_mult"][0],
            "Mul {} argument should be {}".format(0, str(stub["second_mult"][0])))

        # second_mult[1] should be SyntaxTree
        sub_add = second_mult.children[1]
        self.assertIsInstance(
            sub_add,
            SyntaxTree,
            "children[{}] should be instance of SyntaxTree".format(1))
        self.assertEqual(sub_add.operation, "add", "Operation should be 'add'")
        self.assertEqual(len(sub_add.children), 2, "Should have 2 children")

        for index in range(0, 2):
            self.assertIsInstance(
                sub_add.children[index],
                int,
                "children[{}] should be instance of str".format(index))
            self.assertEqual(
                sub_add.children[index],
                stub["second_mult"][1][index],
                "Add {} argument should be {}".format(index, stub["second_mult"][1][index]))

    def test__parse_raise_incorrect_expression_error(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        with self.assertRaises(errors.IncorrectExpression):
            parser._parse("add((1,2)", True)

    def test_run(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(
            config.content["processing"]["transformation"])

        parser.run()

        #self.assertEqual(len(parser.expanded_transformation), 5, 'Transformations should contain 5 elements')

        self.assertEqual(parser.expanded_transformation[1], 'dst_ip',
                         "2 element in expanded transformation should be 'dst_ip'")

        for index in [0, 2, 3]:
            self.assertIsInstance(
                parser.expanded_transformation[index],
                FieldTransformation,
                "{} element expanded transformation should has FieldTransformation type".format(index))

            self.assertEqual(
                parser.expanded_transformation[index].name,
                stub['run_test'][index]['field_name'],
                "expanded_transformation[{}].field_name should be {}".format(
                    index,
                    stub["run_test"][index]["field_name"]))

            self.assertIsInstance(parser.expanded_transformation[index].body, stub["run_test"][index]["type"],
                                  'expanded_transformation[{}].operation should be instance of {}'.format(index, stub[
                                      "run_test"][index]["type"]))
