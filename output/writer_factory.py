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

from errors import errors
from influxdb import InfluxDBClient
from .std_out_writer import StdOutWriter
from .influx_writer import InfluxWriter


class WriterFactory:
    def get_writers(self, output_config, struct, enumerate_input_field):
        main_output = [o for o in output_config.content["outputs"] if o["main"] == True]
        main_output = main_output[0]
        return self.get_writer(main_output, struct, enumerate_input_field)

    def get_writer(self, output, struct, enumerate_input_field):
        if output["method"] == "influx":
            client = InfluxDBClient(output["options"]["influx"]["host"], output["options"]["influx"]["port"],
                                    output["options"]["influx"]["username"], output["options"]["influx"]["password"],
                                    output["options"]["influx"]["database"])

            return InfluxWriter(client, output["options"]["influx"]["database"],
                                output["options"]["influx"]["measurement"], struct, enumerate_input_field)
        elif output["method"] == "stdout":
            return StdOutWriter()

        raise errors.UnsupportedOutputFormat("Format {} not supported".format(output["method"]))
