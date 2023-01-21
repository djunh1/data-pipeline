'''
Created on Jan 19, 2023

@author: douglasjacobson

Reads and stores the data as dataframes
'''

import csv
from datetime import datetime
import os
from pathlib import Path
from uuid import uuid4

import pandas as pd

class DataHelper(object):
    def __init__(self):
        self.file_path = os.path.dirname(Path(__file__).parent) + '/raw_data/'
        self.save_path = os.path.dirname(Path(__file__).parent) + '/temp/'
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

    def generate_csv(self, df, file_name):
        file_name = file_name +'_' + datetime.now().strftime('%Y%m-%d%H-%M%S-') + str(uuid4()) + '.csv'
        df.to_csv(self.save_path + file_name, encoding='utf-8')


    def add_df_to_dict(self, file_name):
        df = pd.read_csv('%s%s.csv' % (self.file_path, file_name) )
        self.df_dict[file_name] = df




