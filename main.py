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

import sys
import logging
from config_parsing.config import Config
from dispatcher.dispatcher import Dispatcher

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            logging.critical("Invalid amount of arguments\nUsage: main.py <config.json>")
            exit(1)

        path_to_config = sys.argv[1].strip()
        config = Config(path_to_config)
        dispatcher = Dispatcher(config, path_to_config)
        dispatcher.run_pipeline()
        dispatcher.stop_pipeline()
    except KeyboardInterrupt:
        logging.warning("You terminated execution.")
        exit(2)
    except BaseException as ex:
        logging.exception(ex)
        exit(1)
