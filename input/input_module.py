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

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from errors.errors import InputError, KafkaConnectError
from .executors import StreamingExecutor
from pyspark.streaming.kafka import KafkaUtils

string_to_int = lambda x: int(x)
string_to_string = lambda x: x
string_to_float = lambda x: float(x)
string_to_boolean = lambda x: bool(x)


def type_to_func(type_field):
    if type_field == IntegerType():
        return string_to_int

    if type_field == BooleanType():
        return string_to_boolean

    if type_field == LongType():
        return string_to_int

    if type_field == StringType():
        return string_to_string

    if type_field == DoubleType():
        return string_to_float

    if type_field == FloatType():
        return string_to_float


class InputConfig:
    """
    InputConfig is a class for reading config file to input module.
    """

    def __init__(self, path_to_config):
        """
        Create a new InputConfig. A param path_to_config should be set.
        :param path_to_config:  Relative path to config file.
        """
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)


class ReadFactory():
    """
    Main entry point to input module. A ReadFactory create executable for different
    configuration/
    """

    def __init__(self, input_config, file_config):
        """
        Create ReadFactory with set config file

        :param input_config: A object of Config class with input options
        """
        self._config = input_config
        self._file_config = file_config

    def get_executor(self):
        """
        The getExecutor create executor depending input config file
        :return:
        """
        if "input" in self._config.content.keys():
            if self._config.content["input"]["input_type"] == "kafka":
                return KafkaStreaming(self._config, self._file_config).get_streaming_executor()
            raise InputError("Error: {} unsuported input format. ReadFactory cannot create Executable".format(
                self._config.content["input"]))
        raise InputError("Error: Some option was miss in config file. ReadFactory cannot create Executable")


class KafkaStreaming(object):
    def __init__(self, config, file_config):

        self._server = config.content["input"]["options"]["server"]
        self._port = config.content["input"]["options"]["port"]
        self._topic = config.content["input"]["options"]["topic"]
        self._consumer_group = config.content["input"]["options"]["consumer_group"]
        self._batchDuration = config.content["input"]["options"]["batchDuration"]
        self._sep = config.content["input"]["options"]["sep"]

        self._spark = SparkSession.builder.appName("StreamingDataKafka").getOrCreate()
        sc = self._spark.sparkContext

        # database files registration
        for name, file in config.content["databases"].items():
            sc.addFile(file)

        # configuration file registration
        sc.addFile(file_config)

        self._ssc = StreamingContext(sc, self._batchDuration)

        list_conversion_function = list((map(lambda x: type_to_func(x.dataType), config.data_structure_pyspark)))
        ranked_pointer = list(enumerate(list_conversion_function))
        functions_list = list(map(lambda x: lambda list_string: x[1](list_string[x[0]]), ranked_pointer))
        function_convert = lambda x: list(map(lambda func: func(x), functions_list))
        try:
            dstream = KafkaUtils.createStream(
                self._ssc,
                "{0}:{1}".format(self._server, self._port),
                self._consumer_group,
                {self._topic: 1})
            self._dstream = dstream.map(lambda x: function_convert(x[1].split(",")))
        except:
            raise KafkaConnectError("Kafka error: Connection refused: server={} port={} consumer_group={} topic={}".
                                    format(self._server, self._port, self._consumer_group, self._topic))

    def get_streaming_executor(self):
        """
            getExecutable return Executor object
        """
        return StreamingExecutor(self._dstream, self._ssc)
