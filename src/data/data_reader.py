'''
Created on Jan 19, 2023

@author: douglasjacobson

Reads and stores the data as dataframes
'''

import csv
import os
from pathlib import Path

import pandas as pd

class DataReader(object):
    def __init__(self):
        self.file_path = os.path.dirname(Path(__file__).parent) + '/raw_data/'
        self.csv_dict = {}
        self.df_dict = {}

    def add_csv(self, file_name):
        '''
        Helper method to add data files to a dict
        :param file_name:
        :return: void
        '''
        data_file = open('%s%s.csv' % (self.file_path, file_name) , 'r')
        self.csv_dict[file_name] = data_file

    def add_df_to_dict(self, file_name):
        df = pd.read_csv('%s%s.csv' % (self.file_path, file_name) )
        self.df_dict[file_name] = df




