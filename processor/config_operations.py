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

from functools import lru_cache


class ConfigReader():
    def __init__(self, path, content):
        self.obj = {}
        self.path = path
        self.content = content

    '''
    :param maxsize: is the number of key in cache
    '''
    @lru_cache(maxsize=32768)
    def read(self):
        head, *tail = self.path[1:-1].split(".")
        self.obj = self.content.get(head)
        for k in tail:
            self.obj = self.obj.get(k)
        return self.obj
