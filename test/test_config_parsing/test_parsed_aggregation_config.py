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
from unittest import TestCase

from pyspark.sql import types
from pyspark.sql.types import StringType, StructField, StructType, LongType, IntegerType


from config_parsing.aggregations_parser import AggregationsParser
from errors.errors import NotValidAggregationExpression


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAggregationsParser(TestCase):
    def setUp(self):
        with open(os.path.join(os.path.dirname(__file__),
                               os.path.join("..", "data", "config_data_structure.json"))) as cfg:
            data_structure = json.load(cfg)

        self.data_structure = data_structure
        data_structure_list = list(
            map(lambda x: (x, data_structure[x]), data_structure.keys()))
        data_structure_sorted = sorted(
            data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types.StructType(
            list(map(lambda x: types.StructField(x[0], getattr(types, x[1]["type"])()),
                     data_structure_sorted)))

    def test_get_parse_expression(self):
        test_input_rule = json.loads(
            """["key: input_port","sum(packet_size)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        input_port = StructField('input_port', IntegerType())
        packet_size = StructField('packet_size', LongType())
        test_aggregation_config = AggregationsParser(
            config, StructType([input_port, packet_size, ]))
        test_expression_token = test_aggregation_config.get_parse_expression()
        self.assertIsInstance(test_expression_token, dict,
                              "Return value of the get_parse_expression method should be instance of dict")
        self.assertEqual(test_expression_token["operation_type"], "reduceByKey",
                         "The dictionary should be contain pair 'operation_type':'reduce'")
        self.assertIsInstance(test_expression_token["rule"], list,
                              "The dictionary should be contain not empty pair 'rule':list of token")
        self.assertGreater(len(test_expression_token["rule"]), 0,
                           "The dictionary should be contain not empty pair 'rule':list of token")

        # test exception to incorrect type,function name or field name
        test_input_rule = json.loads(
            """["key : field_name1","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)

        with self.assertRaisesRegexp(NotValidAggregationExpression, "^Unsupported function\(s\): {'count'}$"):
            test_expression_token = test_aggregation_config.get_parse_expression()

    def test__field_validation(self):
        config = TestConfig({"processing": {"aggregations": {"operation_type": "",
                                                             "rule": ""}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)

        test_parse = test_aggregation_config._field_validation([('count', 'field_name2')],
                                                               "count(field_name2):new_field_name2")
        self.assertIsInstance(test_parse, dict,
                              "Return value of the _field_validation method should be instance of dict")
        self.assertDictEqual(test_parse,
                             {'func_name': 'count',
                                 'input_field': 'field_name2', 'key': False},
                             "Dictionary should contain next pair:func_name: value, input_field: "
                             "value")

        # test exception when find 2 and more regexp in field
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_parse = test_aggregation_config._field_validation([('count', 'field_name2'), ('sum', 'field_name3')],
                                                                   "count(field_name2):new_field_name2")
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        # test exception when don't find regexp in field
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_parse = test_aggregation_config._field_validation([], "")
        self.assertTrue("Error in the field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__pars_reduce(self):
        test_input_rule = json.loads(
            """["Min(field_name1)","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_expression_token = test_aggregation_config._parse_reduce()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for special characters
        #
        test_input_rule = json.loads(
            """["sum(field_name1)#","min(key)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Invalid characters detected" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = json.loads(
            """["sum(field_name1) sdfsdf","min(key)","sum(field_nameN)"]""")
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__check_uniq_key_field(self):
        test_input_rule = json.loads(
            """["min(field_name1)","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        self.assertTrue(test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": False},
                                                                         {"input_field": "test2", "key": False}]),
                        "Return value should be true if the input list don't contain key fields with true value")
        self.assertTrue(not test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": True},
                                                                             {"input_field": "test2", "key": False}]),
                        "Return value should be false if the input list contain key fields with true value")

    def test__parse_reduce_by_key(self):
        test_input_rule = json.loads(
            """["key : field_name1","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce_by_key method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing complex key
        #

        test_input_rule = json.loads(
            """["key : (field_name1,field_name2)","count(field_name3)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce_by_key method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for two or more key field
        #
        test_input_rule = json.loads(
            """["key : field_name1","key : field_name2","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Key field is not unique in rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing the key field
        #
        test_input_rule = json.loads(
            """["sum(field_name1)","min(key)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("don't contain key field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing parenthesis
        #
        test_input_rule = json.loads(
            """["key: (key_field1, key_field2","sum(field_name1)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("The number of opening and closing parentheses" in context.exception.args[0],
                        "Catch exception, but it differs from test exception {}".format(context.exception.args[0]))

        #
        # Testing an exception for special characters
        #
        test_input_rule = json.loads(
            """["sum(field_name1)#","min(key)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Invalid characters detected" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = json.loads(
            """["sum(field_name1) sdfsdf","min(key)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__parse_expression(self):
        test_input_rule = json.loads(
            """["key : field_name1","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = json.loads(
            """["sum(field_name1)","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = json.loads(
            """["key: field_name1","count(field_name2)","sum(field_nameN)"]""")
        test_input_operation = "groupBy"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)

        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_expression()
        self.assertTrue("The operation" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__types_and_fields_validation_raise_wrong_function_exception(self):
        # test wrong  function name
        test_input_rule = json.loads(
            """["key: input_port","sin(in_vlan)","sum(ip_size)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        input_port = StructField('input_port', IntegerType())
        in_vlan = StructField('in_vlan', IntegerType())
        ip_size = StructField('ip_size', IntegerType())
        test_aggregation_config = AggregationsParser(
            config, StructType([input_port, in_vlan, ip_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Unsupported function\(s\): {'sin'}$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_wrong_field_name_exception(self):
        # test wrong field name
        test_input_rule = json.loads(
            """["key: input_port","min(in_vlan_bad)","sum(ip_size)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(
            config, self.data_structure_pyspark)
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression,
                                    "^Unsupported or unused field\(s\): {'in_vlan_bad'}$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_already_aggregated_field_exception(self):
        test_input_rule = json.loads(
            """["key: src_ip","max(packet_size)","min(packet_size)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        src_ip = StructField('src_ip', StringType())
        packet_size = StructField('packet_size', LongType())
        test_aggregation_config = AggregationsParser(
            config, StructType([src_ip, packet_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Aggregate already aggregated field packet_size$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_wrong_field_type_exception(self):
        # test wrong  type of field
        test_input_rule = json.loads(
            """["key: input_port","min(dst_mac)","sum(ip_size)"]""")
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        input_port = StructField('input_port', IntegerType())
        ip_size = StructField('ip_size', IntegerType())
        dst_mac = StructField('dst_mac', StringType())
        test_aggregation_config = AggregationsParser(
            config, StructType([input_port, dst_mac, ip_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression,
                                    "^Incorrect type of field dst_mac for function min$"):
            _ = test_aggregation_config._types_and_field_names_validation()
