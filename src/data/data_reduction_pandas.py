'''
Created on Jan 19, 2023

@author: douglasjacobson

Performs data operation using pandas
'''

import pandas as pd

class DataReductionPandas(object):
    def __init__(self, data_reader):
        self.data_reader = data_reader


    def run_data_reduction(self):
        df_dict = {}
        for k, v in self.data_reader.df_dict.items():
            df_dict[k] = v

        df_merged = self.combine_data_frames(df_dict["dataset1"], df_dict['dataset2'])
        df_reduced = self.reduce_data_frame(df_merged)


    def combine_data_frames(self, df1, df2):
        df_merged = pd.merge(df1, df2, on='counter_party')
        df_merged = df_merged.drop(['invoice_id'], axis=1)
        return df_merged


    def reduce_data_frame(self, df):
        df.groupby(['legal_entity']).sum()
        print('test')




