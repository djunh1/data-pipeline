'''
Created on Jan 21, 2023

@author: douglasjacobson

Performs data operation using Apache Beam framework
'''

import os
from pathlib import Path

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.io.filesystems import FileSystems as beam_fs
from apache_beam.options.pipeline_options import PipelineOptions
import codecs
import csv
from typing import Dict, Iterable, List


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--output',
         type=str,  default= os.path.dirname(Path(__file__).parent) + '/temp/' +'test.csv')



class DataReductionApacheBeam(object):
    def __init__(self, data_helper):
        self.data_helper = data_helper


    def run_beam_pipeline(self):

        def split_by_kv(element, index, delimiter=", "):
            splitted = element.split(delimiter)
            print(splitted[index], element)
            return splitted[index], element


        input_file_dict = {}
        for k, v in self.data_helper.csv_dict.items():
            input_file_dict[k] = v

        input_files = self.data_helper.file_path +'ds*.csv'
        options = PipelineOptions(flags=[], type_check_additional='all')

        with beam.Pipeline() as p:
            ds1  = (p | "Read ds1" >> beam.io.ReadFromText(input_file_dict['ds1'].name)
                       | "for ds1" >> beam.Map(split_by_kv, index=2, delimiter=",")
                      )

            ds2 = (p | "Read ds2" >> beam.io.ReadFromText(input_file_dict['ds2'].name)
                     | "to KV order" >> beam.Map(split_by_kv, index=0, delimiter=",")
                     )


            # merged = ((ds1, ds2) | 'Merge PCollections' >> beam.Flatten()
            #                     | beam.Filter(lambda line: line[1] == 'L1')
            #                     | beam.Map(lambda line: (line[0], 1))
            #                     | 'group '>> beam.GroupByKey()
            #                     | beam.Map(print)
            #                     #| beam.io.WriteToText(self.data_helper.save_path + 'test.txt')
            #           )


            ({ ds1,  ds2} | beam.CoGroupByKey()
                                           | beam.Map(print)
            )


