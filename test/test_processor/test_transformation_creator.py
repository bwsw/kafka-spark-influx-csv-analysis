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
import os
import types
from pyspark.sql import types as types_spark
from unittest import TestCase

from pyspark.sql import SparkSession

from config_parsing.transformations_parser import FieldTransformation, SyntaxTree, TransformationsParser
from config_parsing.transformations_validator import TransformationsValidator
from operations.transformation_operations import TransformationOperations
from processor.transformation_creator import TransformationCreator

DATA_PATH = os.path.join(
    os.path.dirname(__file__),
    os.path.join("..", "data", "test.csv"))

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    os.path.join("..", "data", "config.json"))


class TransformationCreatorTestCase(TestCase):
    def setUp(self):
        with open(os.path.join(
                os.path.dirname(__file__),
                os.path.join("..", "data", "config_data_structure.json"))) as cfg:
            data_structure = json.load(cfg)
        with open(os.path.join(
                os.path.dirname(__file__),
                os.path.join("..", "data", "config.json"))) as cfg:
            self.config = json.load(cfg)

        self.data_structure = data_structure
        data_structure_list = list(
            map(lambda x: (x, data_structure[x]), data_structure.keys()))
        data_structure_sorted = sorted(
            data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types_spark.StructType(
            list(map(lambda x: types_spark.StructField(x[0], getattr(types_spark, x[1]["type"])()),
                     data_structure_sorted)))

    def test_build_lambda(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mul"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("traffic", mult_syntax_tree)]

        creator = TransformationCreator(
            self.data_structure, parsed_transformations, TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 37888),
                                      ("91.221.61.168", "90.188.114.141", 34816),
                                      ("91.226.13.80", "5.136.78.36", 773120),
                                      ("192.168.30.2", "192.168.30.1", 94720),
                                      ("192.168.30.2", "192.168.30.1", 94720)], "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_with_nested_operations(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mul"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        root_mult_st = SyntaxTree()
        root_mult_st.operation = "mul"
        root_mult_st.children = [mult_syntax_tree, "10"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("traffic", root_mult_st)]
        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 378880),
                                      ("91.221.61.168", "90.188.114.141", 348160),
                                      ("91.226.13.80", "5.136.78.36", 7731200),
                                      ("192.168.30.2", "192.168.30.1", 947200),
                                      ("192.168.30.2", "192.168.30.1", 947200)],
                             "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_with_literals(self):
        st = SyntaxTree()
        st.operation = "concat"
        st.children = ["'6 - '", "packet_size"]  # packet_size [74, 68]

        parsed_transformations = [
            FieldTransformation("ephemer", st)]

        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))
        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result,
            [
                ("6 - 74",),
                ("6 - 68",),
                ("6 - 1510",),
                ("6 - 185",),
                ("6 - 185",)], "List of tuples should be equal")

    def test_build_lambda_with_nested_literals(self):
        st = SyntaxTree()
        st.operation = "concat"
        # should cast int to str and concat
        st.children = ["'6'", "packet_size"]  # packet_size [74, 68]

        st2 = SyntaxTree()
        st2.operation = "concat"
        st2.children = [2E+2, st]

        parsed_transformations = [
            FieldTransformation("nested", st2)]
        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))
        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result,
            [
                ('200.0674',),
                ('200.0668',),
                ('200.061510',),
                ('200.06185',),
                ('200.06185',)], "List of tuples should be equal")
        spark.stop()

    def test_build_lambda_concat_with_nested_mul(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mul"
        mult_syntax_tree.children = [6, "packet_size"]
        mult_syntax_tree_root = SyntaxTree()
        mult_syntax_tree_root.operation = "concat"
        mult_syntax_tree_root.children = [
            mult_syntax_tree, "' -- xe \' 2/3 mul(3,3) FooBar'"]

        parsed_transformations = [
            FieldTransformation("traffic", mult_syntax_tree_root)]
        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result, [
                ('444 -- xe \' 2/3 mul(3,3) FooBar',),
                ('408 -- xe \' 2/3 mul(3,3) FooBar',),
                ('9060 -- xe \' 2/3 mul(3,3) FooBar',),
                ('1110 -- xe \' 2/3 mul(3,3) FooBar',),
                ('1110 -- xe \' 2/3 mul(3,3) FooBar',)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_truncate(self):
        st = SyntaxTree()
        st.operation = "truncate"
        st.children = ["'test'", 2]

        parsed_transformations = [
            FieldTransformation("cut_upto_2_symbols", st)]
        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result, [('te',), ('te',), ('te',), ('te',), ('te',)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_add_scientific(self):
        st = SyntaxTree()
        st.operation = "add"
        st.children = [1.2E+5, 1.0]
        parsed_transformations = [
            FieldTransformation("sum", st)]
        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result, [(120001.0,), (120001.0,), (120001.0,),
                     (120001.0,), (120001.0,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_numbers(self):
        st = SyntaxTree()
        st.operation = "_"
        st.children = [13]  # as if it parsed

        parsed_transformations = [
            FieldTransformation("a", st)]

        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parsed_transformations)

        creator = TransformationCreator(self.data_structure, parsed_transformations,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result, [(13,), (13,), (13,), (13,), (13,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_numbers(self):
        parser = TransformationsParser(["dst_ip: 13"])
        parser.run()

        operations = TransformationOperations(self.config)
        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()
        self.assertListEqual(
            result, [(13,), (13,), (13,), (13,), (13,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_int_unsigned(self):
        parser = TransformationsParser(["dst_ip: +13"])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [(13,), (13,), (13,), (13,), (13,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_int_neg(self):
        parser = TransformationsParser(["dst_ip: -13"])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)

        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [(-13,), (-13,), (-13,), (-13,), (-13,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_float_neg(self):
        parser = TransformationsParser(["dst_ip: -13.5"])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [(-13.5,), (-13.5,), (-13.5,), (-13.5,), (-13.5,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_add(self):
        self.maxDiff = None
        parser = TransformationsParser(
            [
                "dst_ip: add(-13.5, 2)",
                "src_ip:add(-13.5,2)",
                "foobar: 'add(-13.5,2)'",
                "foobar2: 'add\\'(-13.5,2)'"
            ])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [
                (-11.5, -11.5, 'add(-13.5,2)', "add'(-13.5,2)"),
                (-11.5, -11.5, 'add(-13.5,2)', "add'(-13.5,2)"),
                (-11.5, -11.5, 'add(-13.5,2)', "add'(-13.5,2)"),
                (-11.5, -11.5, 'add(-13.5,2)', "add'(-13.5,2)"),
                (-11.5, -11.5, 'add(-13.5,2)', "add'(-13.5,2)")
            ], "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_str(self):
        parser = TransformationsParser(["a: '-13.5'"])
        parser.run()

        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [('-13.5',), ('-13.5',), ('-13.5',),
                     ('-13.5',), ('-13.5',)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_bool(self):
        parser = TransformationsParser([
            "dst_ip: True",
            "src_ip: False"])
        parser.run()

        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [
                (True, False),
                (True, False),
                (True, False),
                (True, False),
                (True, False)], "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_config(self):
        parser = TransformationsParser(["a: config('input.options.port')"])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)

        _ = transformations_validator.validate(parser.expanded_transformation)
        creator = TransformationCreator(self.data_structure, parser.expanded_transformation,
                                        TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [(29092,), (29092,), (29092,), (29092,), (29092,)],
            "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_processor_nested_config(self):
        parser = TransformationsParser([
            "bd:config('input.options.batchDuration')",
            "port:config('input.options.port')",
            "concat:concat('Port - ',concat(3,config('input.options.port')))",
            "concat:add(10,int(config('input.options.port')))"
        ])
        parser.run()
        operations = TransformationOperations(self.config)

        transformations_validator = TransformationsValidator(
            operations, self.data_structure)
        _ = transformations_validator.validate(parser.expanded_transformation)

        creator = TransformationCreator(
            self.data_structure,
            parser.expanded_transformation,
            TransformationOperations(self.config))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType,
                              "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(
            result, [
                (10, 29092, 'Port - 329092', 29102),
                (10, 29092, 'Port - 329092', 29102),
                (10, 29092, 'Port - 329092', 29102),
                (10, 29092, 'Port - 329092', 29102),
                (10, 29092, 'Port - 329092', 29102)],
            "List of tuples should be equal")

        spark.stop()
