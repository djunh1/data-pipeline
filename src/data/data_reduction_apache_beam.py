'''
Created on Jan 21, 2023

@author: douglasjacobson

Performs data operation using Apache Beam framework
'''

import os
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv


from csv import reader





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

        p = beam.Pipeline()
        ds1 = p | 'dataseries_1' >> read_csv(input_file_dict['dataset1'].name)
        ds2 = p | 'dataseries_2' >> read_csv(input_file_dict['dataset2'].name)

        result = p.run()
        result.wait_until_finish()


        current_data_headers = self.read_headers(input_file_dict['dataset1'].name)
        # with beam.Pipeline() as p:
        #     ds1 = (p | "Read orders" >> beam.io.ReadFromText(input_file_dict['dataset1'].name, skip_header_lines=1)
        #               #| "to KV order" >> beam.Map(merge_dataframes.split_by_kv, index=1, delimiter=",")
        #               )
        #
        #     ds2 = (p | "Read users" >> beam.io.ReadFromText(input_file_dict['dataset2'].name, skip_header_lines=1)
        #              #| "to KV users" >> beam.Map(merge_dataframes.split_by_kv, index=0, delimiter=",")
        #              )
        #
        #     ({"ds1": ds1, "ds2": ds2} | beam.CoGroupByKey()
        #      | beam.Map(print)
        #      )




        # p = beam.Pipeline()
        # concating = (p
        #              | beam.io.fileio.MatchFiles('%s*.csv' % self.data_helper.file_path)
        #              | beam.io.fileio.ReadMatches()
        #              | beam.Reshuffle()
        #              | beam.ParDo(convert_to_dataFrame())
        #              | beam.combiners.ToList()
        #              | beam.ParDo(merge_dataframes())
        #              | "Write sample text" >> beam.io.WriteToText(self.data_helper.save_path + "a-test.txt")
        #              )
        #
        # p.run()

        # concating = (p
        #              | beam.io.fileio.MatchFiles('%s*.csv' % self.data_helper.file_path)
        #              | beam.io.fileio.ReadMatches()
        #              | beam.ParDo(merge_dataframes())
        #              | "Write sample text" >> beam.io.WriteToText(self.data_helper.save_path + "JustTest.csv"))
        #              #| beam.io.WriteToText(self.data_helper.save_path, file_name_suffix='.csv'))
        # p.run()




        # with beam.Pipeline() as pipeline:
        #     ds1 = (pipeline | "Read ds1" >> read_csv(input_file_dict['dataset1'].name))
        #     ds2 = (pipeline | "Read ds2" >> read_csv(input_file_dict['dataset2'].name))
        #
        #     results = ({'ds1': ds1, 'ds2': ds2} | beam.CoGroupByKey())




        #     ds1 = (pipeline | "Read ds1" >> beam.dataframe.io.read_csv(input_file_dict['dataset1'].name)
        #                | "to KV order" >> beam.Map(self.split_by_kv, index=1, delimiter=",")
        #               )
        #     ds2 = (pipeline | "Read ds2" >> beam.dataframe.io.read_csv(input_file_dict['dataset2'].name)
        #            | "to KV order" >> beam.Map(self.split_by_kv, index=1, delimiter=",")
        #            )
        #     (
        #             convert.to_pcollection(lines)
        #             | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))
        #             | 'Print' >> beam.Map(print)
        #     )


        print('end')


    def split_by_kv(self, element, index, delimiter=", "):
        splitted = element.split(delimiter)
        return splitted[index], element

    def read_headers(self, csv_file):
        with open(csv_file, 'r') as f:
            header_line = f.readline().strip()
        return next(reader([header_line]))
