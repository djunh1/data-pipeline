'''
Created on Jan 21, 2023

@author: douglasjacobson

Performs data operation using Apache Beam framework
'''

import os
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from csv import reader

from apache_beam.dataframe.io import read_csv
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--output',
         type=str,  default= os.path.dirname(Path(__file__).parent) + '/temp/' +'test.csv')



class DataReductionApacheBeam(object):
    def __init__(self, data_helper):
        self.data_helper = data_helper


    def run_beam_pipeline(self):
        input_file_dict = {}
        for k, v in self.data_helper.csv_dict.items():
            input_file_dict[k] = v

        with beam.Pipeline() as p:
            ip = (p
                  | beam.io.ReadFromText(input_file_dict['ds1'].name, skip_header_lines=True)
                  | beam.Map(lambda x: x.split(','))
                  | beam.Filter(lambda x: x[1] == 'L1')
                  | beam.combiners.Count.Globally()
                  | beam.Map(print)
                  )




    def split_by_kv(self, element, index, delimiter=", "):
        splitted = element.split(delimiter)
        return splitted[index], element

    def read_headers(self, csv_file):
        with open(csv_file, 'r') as f:
            header_line = f.readline().strip()
        return next(reader([header_line]))
