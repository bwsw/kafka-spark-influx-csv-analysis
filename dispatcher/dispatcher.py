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

from analysis.analysis_factory import AnalysisFactory
from input.input_module import ReadFactory
from output.writer_factory import WriterFactory
from processor.processor import Processor


class Dispatcher:
    def __init__(self, config):
        self.executor = ReadFactory(config).get_executor()
        self.processor = Processor(config)
        self.writers = WriterFactory().get_writers(config, self.processor.aggregation_output_struct,
                                                      self.processor.enumerate_output_aggregation_field)
        self._isAnalysis = False

        if "analysis" in config.content.keys():
            self._isAnalysis = True
            self.analysis = AnalysisFactory(config,
                                            self.processor.aggregation_output_struct,
                                            self.processor.enumerate_output_aggregation_field)

    def run_pipeline(self):
        processor_part = self.processor.get_pipeline_processing()

        write_funcs = [w.get_write_lambda() for w in self.writers]
        write_func = lambda rdd: [w(rdd) for w in write_funcs]

        # pipeline = lambda rdd: write_lambda(processor_part(rdd))
        if self._isAnalysis:
            analysis_lambda = self.analysis.get_analysis_lambda()
            pipeline = lambda rdd: self._all_pipeline(rdd, processor_part, write_func, analysis_lambda)
        else:
            analysis_lambda = lambda x: x
            pipeline = lambda rdd: self._all_pipeline(rdd, processor_part, write_func, analysis_lambda)

        self.executor.set_pipeline_processing(pipeline)
        self.executor.run_pipeline()

    def _all_pipeline(self, rdd, processor_part, write_part, analysis_part):
        processed = processor_part(rdd)
        write_part(processed)
        analysis_part(processed)

    def stop_pipeline(self):
        self.executor.stop_pipeline()
