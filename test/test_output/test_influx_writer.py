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
from unittest import TestCase
from unittest.mock import Mock

from pyspark.sql import SparkSession
from config_parsing.config import Config
from output.influx_writer import InfluxWriter

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_influx.json"))


class InfluxDBClientMock():
    def __new__(cls, host, port, username, password, database):
        mock = Mock()

        mock.write_points.side_effect = lambda points: cls.__save_points_in_mock(mock, points)
        mock.query.side_effect = lambda query: cls.__get_result_set_from_mock(mock)

        return mock

    @staticmethod
    def __save_points_in_mock(mock, points):
        mock.points = points

    @staticmethod
    def __get_result_set_from_mock(mock):
        result_wrapper = Mock()

        influx_points = []
        for point in mock.points:
            d, d["time"] = {**point["fields"], **point.get("tags", {})}, point["time"]
            influx_points.append(d)

        result_wrapper.get_points.return_value = influx_points
        return result_wrapper


class InfluxWriterTestCase(TestCase):
    def tearDown(self):
        self.__class__.writer.client.drop_database(self.__class__.influx_options["database"])

    def test_write_tuple_to_influx(self):
        struct = {'operation_type': 'reduce',
                  'rule': [{'key': False, 'input_field': 'packet_size', 'func_name': 'Min'},
                           {'key': False, 'input_field': 'traffic', 'func_name': 'Max'},
                           {'key': False, 'input_field': 'traffic2', 'func_name': 'Sum'}]}
        enumerate_output_aggregation_field = {"packet_size": 0, "traffic": 1, "traffic2": 2}
        config = Config(CONFIG_PATH)
        self.__class__.influx_options = config.content["outputs"][0]["options"]["influx"]

        client = InfluxDBClientMock(self.__class__.influx_options["host"], self.__class__.influx_options["port"],
                                    self.__class__.influx_options["username"],
                                    self.__class__.influx_options["password"],
                                    self.__class__.influx_options["database"])

        self.__class__.writer = InfluxWriter(client, self.__class__.influx_options["database"],
                                             self.__class__.influx_options["measurement"],
                                             struct, enumerate_output_aggregation_field)

        write_lambda = self.__class__.writer.get_write_lambda()
        t = (2, 3, 5)
        write_lambda(t)

        result = self.__class__.writer.client.query(
            "select * from {0}".format(self.__class__.influx_options["measurement"]))
        points = list(result.get_points())

        self.assertEqual(len(points), 1,
                         "In {0} measurement should be written one point".format(
                             self.__class__.influx_options["measurement"]))

        fields = [field["input_field"] for field in struct["rule"] if not field["key"]]
        for index, name in enumerate(fields):
            self.assertEqual(points[0][name], t[index], "Value should be {0}".format(t[index]))

    def test_write_number_to_influx(self):
        struct = {'operation_type': 'reduce',
                  'rule': [{'key': False, 'input_field': 'packet_size', 'func_name': 'Min'}]}
        enumerate_output_aggregation_field = {"packet_size": 0}
        config = Config(CONFIG_PATH)
        self.__class__.influx_options = config.content["outputs"][0]["options"]["influx"]

        client = InfluxDBClientMock(self.__class__.influx_options["host"], self.__class__.influx_options["port"],
                                    self.__class__.influx_options["username"],
                                    self.__class__.influx_options["password"],
                                    self.__class__.influx_options["database"])

        self.__class__.writer = InfluxWriter(client, self.__class__.influx_options["database"],
                                             self.__class__.influx_options["measurement"],
                                             struct, enumerate_output_aggregation_field)

        write_lambda = self.__class__.writer.get_write_lambda()
        write_lambda(6)

        result = self.__class__.writer.client.query(
            "select * from {0}".format(self.__class__.influx_options["measurement"]))
        points = list(result.get_points())

        self.assertEqual(len(points), 1,
                         "In {0} measurement should be written one point".format(
                             self.__class__.influx_options["measurement"]))

        self.assertEqual(points[0]["packet_size"], 6, "Value should be 6")
